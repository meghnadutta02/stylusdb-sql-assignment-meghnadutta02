const { parseQuery } = require("./queryParser");
const readCSV = require("./csvReader");

function performInnerJoin(data, joinData, joinCondition) {
  const leftColumn = joinCondition.left.split(".")[1];
  const leftTable = joinCondition.left.split(".")[0];

  const rightColumn = joinCondition.right.split(".")[1];
  const rightTable = joinCondition.right.split(".")[0];
  const rKeys = Object.keys(joinData[0]);
  const lKeys = Object.keys(data[0]);
  const result = data.reduce((acc, row) => {
    // Find all matching rows in joinData based on the join condition
    const matchingJoinRows = joinData.filter(
      (joinRow) => row[leftColumn] === joinRow[rightColumn]
    );

    // If matching rows are found in joinData, merge data from both tables for each match
    if (matchingJoinRows.length > 0) {
      matchingJoinRows.forEach((matchingJoinRow) => {
        let newRow = {};
        lKeys.forEach((key) => {
          newRow[`${leftTable}.${key}`] = row[key];
        });
        rKeys.forEach((key) => {
          newRow[`${rightTable}.${key}`] = matchingJoinRow[key];
        });
        acc.push(newRow);
      });
    }
    return acc;
  }, []);

  return result;
}

function performLeftJoin(data, joinData, joinCondition) {
  const leftColumn = joinCondition.left.split(".")[1];
  const leftTable = joinCondition.left.split(".")[0];
  const rightColumn = joinCondition.right.split(".")[1];
  const rightTable = joinCondition.right.split(".")[0];
  const lKeys = Object.keys(data[0]);
  const rKeys = Object.keys(joinData[0]);

  const result = data.reduce((acc, row) => {
    // Find all matching rows in joinData based on the join condition
    const matchingJoinRows = joinData.filter(
      (joinRow) => row[leftColumn] === joinRow[rightColumn]
    );

    // If matching rows are found in joinData, merge data from both tables for each match
    if (matchingJoinRows.length > 0) {
      matchingJoinRows.forEach((matchingJoinRow) => {
        let newRow = {};
        lKeys.forEach((key) => {
          newRow[`${leftTable}.${key}`] = row[key];
        });
        rKeys.forEach((key) => {
          newRow[`${rightTable}.${key}`] = matchingJoinRow[key];
        });
        acc.push(newRow);
      });
    } else {
      let newRow = {};
      lKeys.forEach((key) => {
        newRow[`${leftTable}.${key}`] = row[key];
      });
      rKeys.forEach((key) => {
        newRow[`${rightTable}.${key}`] = null;
      });
      acc.push(newRow);
    }

    return acc;
  }, []);

  return result;
}

function performRightJoin(data, joinData, joinCondition) {
  const leftTable = joinCondition.left.split(".")[0];
  const leftColumn = joinCondition.left.split(".")[1];
  const rightTable = joinCondition.right.split(".")[0];
  const rightColumn = joinCondition.right.split(".")[1];
  const lKeys = Object.keys(data[0]);
  const rKeys = Object.keys(joinData[0]);
  const result = joinData.reduce((acc, joinRow) => {
    // Find all matching rows in data based on the join condition
    const matchingDataRows = data.filter(
      (row) => joinRow[rightColumn] === row[leftColumn]
    );

    // If matching rows are found in data, merge data from both tables for each match
    if (matchingDataRows.length > 0) {
      matchingDataRows.forEach((matchingDataRow) => {
        let newRow = {};

        lKeys.forEach((key) => {
          newRow[`${leftTable}.${key}`] = matchingDataRow[key];
        });
        rKeys.forEach((key) => {
          newRow[`${rightTable}.${key}`] = joinRow[key];
        });
        acc.push(newRow);
      });
    } else {
      let newRow = {};
      lKeys.forEach((key) => {
        newRow[`${leftTable}.${key}`] = null;
      });
      rKeys.forEach((key) => {
        newRow[`${rightTable}.${key}`] = joinRow[key];
      });
      acc.push(newRow);
    }

    return acc;
  }, []);

  return result;
}
function applyGroupBy(data, groupByFields, fields) {
  const groupedData = {};

  data.forEach((row) => {
    const key =
      groupByFields.length > 0
        ? groupByFields.map((field) => row[field]).join("|")
        : "all";
    if (!groupedData[key]) {
      groupedData[key] = [];
    }
    groupedData[key].push(row);
  });

  const aggregatedData = Object.keys(groupedData).map((key) => {
    const rows = groupedData[key];
    const result = {};
    fields.forEach((field) => {
      const match = /(\w+)\((\*|\w+)\)/.exec(field);
      if (match) {
        const [, aggFunc, aggField] = match;
        switch (aggFunc.toUpperCase()) {
          case "COUNT":
            result[field] = rows.length;
            break;
          case "SUM":
            result[field] = rows.reduce(
              (acc, row) => acc + parseFloat(row[aggField]),
              0
            );
            break;
          case "AVG":
            result[field] =
              rows.reduce((acc, row) => acc + parseFloat(row[aggField]), 0) /
              rows.length;
            break;
          case "MIN":
            result[field] = Math.min(
              ...rows.map((row) => parseFloat(row[aggField]))
            );
            break;
          case "MAX":
            result[field] = Math.max(
              ...rows.map((row) => parseFloat(row[aggField]))
            );
            break;
          case "MEDIAN":
            const sorted = rows
              .map((row) => parseFloat(row[aggField]))
              .sort((a, b) => a - b);
            const mid = Math.floor(sorted.length / 2);
            result[field] =
              sorted.length % 2 !== 0
                ? sorted[mid]
                : (sorted[mid - 1] + sorted[mid]) / 2;
            break;
        }
      } else if (groupByFields.includes(field)) {
        result[field] = rows[0][field];
      }
    });
    return result;
  });

  return aggregatedData;
}

async function executeSELECTQuery(query) {
  const {
    fields,
    table,
    whereClauses,
    joinType,
    joinTable,
    joinCondition,
    groupByFields,
    hasAggregateWithoutGroupBy,
  } = parseQuery(query);
  console.log("where clause", whereClauses);
  let data = await readCSV(`${table}.csv`);
  if (joinTable && joinCondition) {
    const joinData = await readCSV(`${joinTable}.csv`);
    switch (joinType.toUpperCase()) {
      case "INNER":
        data = performInnerJoin(data, joinData, joinCondition);
        break;
      case "LEFT":
        data = performLeftJoin(data, joinData, joinCondition);
        break;
      case "RIGHT":
        data = performRightJoin(data, joinData, joinCondition);
        break;
      default:
        throw new Error(`Unsupported join type: ${joinType}`);
    }
  }
  function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
      case "=":
        if (typeof value === "string") {
          return row[field] === value.replace(/['"]/g, "");
        }
        return row[field] === value;
      case "!=":
        return row[field] !== value;
      case ">":
        return row[field] > value;
      case "<":
        return row[field] < value;
      case ">=":
        return row[field] >= value;
      case "<=":
        return row[field] <= value;
      default:
        throw new Error(`Unsupported operator: ${operator}`);
    }
  }
  // Apply WHERE clause filtering after JOIN (or on the original data if no join)
  let filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => evaluateCondition(row, clause))
        )
      : data;

  if (groupByFields && !hasAggregateWithoutGroupBy) {
    filteredData = applyGroupBy(filteredData, groupByFields, fields);
  }
  if (hasAggregateWithoutGroupBy) {
    filteredData = applyGroupBy(filteredData, [], fields);
  }
  return filteredData.map((row) => {
    const selectedRow = {};
    fields.forEach((field) => {
      selectedRow[field] = row[field];
    });
    return selectedRow;
  });
}

module.exports = executeSELECTQuery;
