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

async function executeSELECTQuery(query) {
  const { fields, table, whereClauses, joinType, joinTable, joinCondition } =
    parseQuery(query);

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
          return row[field] === value.replace(/'/g, "");
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
  const filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => evaluateCondition(row, clause))
        )
      : data;

  // Select the specified fields

  return filteredData.map((row) => {
    const selectedRow = {};
    fields.forEach((field) => {
      selectedRow[field] = row[field];
    });
    return selectedRow;
  });
}

module.exports = executeSELECTQuery;
