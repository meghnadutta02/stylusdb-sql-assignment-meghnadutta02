function parseQuery(query) {
  // First, let's trim the query to remove any leading/trailing whitespaces
  try {
    query = query.trim();
    const whereIndex = query.toUpperCase().indexOf(" WHERE ");
    const groupByIndex = query.toUpperCase().indexOf(" GROUP BY ");
    const orderByIndex = query.toUpperCase().indexOf(" ORDER BY ");
    const limitIndex = query.toUpperCase().indexOf(" LIMIT ");
    let isDistinct = false;
    let selectPart = query;
    let whereClause = null;
    let groupByClause = null;
    let limit = null;
    let orderByClause = null;
    let orderByFields = null;

    if (limitIndex !== -1) {
      limit = query.substring(limitIndex + 6).trim();
      selectPart = query.substring(0, limitIndex);
      limit = parseInt(limit, 10);
    }
    if (orderByIndex !== -1) {
      orderByClause = selectPart.substring(orderByIndex + 9).trim();
      selectPart = selectPart.substring(0, orderByIndex);
    }
    if (groupByIndex !== -1) {
      groupByClause = selectPart.substring(groupByIndex + 9).trim();
      selectPart = selectPart.substring(0, groupByIndex);
    }

    if (whereIndex !== -1) {
      whereClause = selectPart.substring(whereIndex + 7).trim();
      selectPart = selectPart.substring(0, whereIndex);
    }

    const joinSplit = selectPart.split(/\s(INNER|LEFT|RIGHT)\s(JOIN)\s/i);
    selectPart = joinSplit[0].trim();
    const joinClause =
      joinSplit.length > 1 ? joinSplit.slice(1).join(" ") : null;

    let [joinType, joinTable, joinCondition] = [null, null, null];
    if (joinClause) {
      ({ joinType, joinTable, joinCondition } = parseJoinClause(joinClause));
    }

    // Parse the SELECT part
    if (selectPart.startsWith("SELECT DISTINCT")) {
      isDistinct = true;
      selectPart = selectPart.replace("SELECT DISTINCT", "SELECT");
    }
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+?)(?:\s|$)/i;
    const selectMatch = selectPart.match(selectRegex);
    if (!selectMatch) {
      throw new Error("Invalid SELECT format");
    }

    const [, fields, table] = selectMatch;

    // Parse the WHERE part if it exists
    let whereClauses = [];
    if (whereClause) {
      whereClauses = parseWhereClause(whereClause);
    }

    let groupByFields = null;
    if (groupByClause) {
      groupByFields = groupByClause.split(",").map((field) => field.trim());
    }

    const aggregateFunctionRegex =
      /(\bCOUNT\b|\bAVG\b|\bSUM\b|\bMIN\b|\bMAX\b)\s*\(\s*(\*|\w+)\s*\)/i;

    const hasAggregateWithoutGroupBy =
      aggregateFunctionRegex.test(selectPart) && !groupByFields;

    if (orderByClause) {
      orderByFields = orderByClause.split(",").map((field) => {
        const [fieldName, order] = field.trim().split(/\s+/);
        return { fieldName, order: order ? order.toUpperCase() : "ASC" };
      });
    }
    return {
      fields: fields.split(",").map((field) => field.trim()),
      table: table.trim(),
      whereClauses,
      joinType,
      joinTable,
      joinCondition,
      groupByFields,
      hasAggregateWithoutGroupBy,
      orderByFields,
      limit,
      isDistinct,
    };
  } catch (e) {
    throw new Error(`Error executing query: Query parsing error: ${e.message}`);
  }
}

function parseJoinClause(query) {
  try {
    const joinRegex =
      /(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
      return {
        joinType: joinMatch[1].trim(),
        joinTable: joinMatch[2].trim(),
        joinCondition: {
          left: joinMatch[3].trim(),
          right: joinMatch[4].trim(),
        },
      };
    }

    return {
      joinType: null,
      joinTable: null,
      joinCondition: null,
    };
  } catch (error) {
    throw new Error(`Join clause parsing error: ${error.message}`);
  }
}
function parseWhereClause(whereString) {
  const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
  return whereString.split(/ AND | OR /i).map((conditionString) => {
    if (conditionString.toUpperCase().includes(" LIKE ")) {
      const [field, pattern] = conditionString.split(/\sLIKE\s/i);
      const p = pattern.replace(/'/g, "");
      return {
        field: field.trim(),
        operator: "LIKE",
        value: p.trim(),
      };
    }
    const match = conditionString.match(conditionRegex);
    if (match) {
      const [, field, operator, value] = match;
      return { field: field.trim(), operator, value: value.trim() };
    }

    throw new Error("Invalid WHERE clause format");
  });
}

module.exports = { parseQuery, parseJoinClause };
