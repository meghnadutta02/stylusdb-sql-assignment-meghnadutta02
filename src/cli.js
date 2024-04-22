#!/usr/bin/env node
const readline = require("readline");
const {
  executeSELECTQuery,
  executeINSERTQuery,
  executeDELETEQuery,
} = require("./index");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

rl.setPrompt("SQL> ");
console.log(
  'SQL Query Engine CLI. Enter your SQL commands, or type "exit" to quit.'
);

rl.prompt();

function identifyQueryType(query) {
  // Regular expressions to match SQL query patterns
  const selectRegex = /^SELECT/i;
  const insertRegex = /^INSERT/i;
  const deleteRegex = /^DELETE/i;

  if (selectRegex.test(query)) {
    return "SELECT";
  } else if (insertRegex.test(query)) {
    return "INSERT";
  } else if (deleteRegex.test(query)) {
    return "DELETE";
  } else {
    throw new Error("Unsupported query type");
  }
}

rl.on("line", async (line) => {
  if (line.toLowerCase() === "exit") {
    rl.close();
    return;
  }

  try {
    const queryType = identifyQueryType(line);

    let result;
    switch (queryType) {
      case "SELECT":
        result = await executeSELECTQuery(line);
        console.log("Query Result:", result);
        break;
      case "INSERT":
        result = await executeINSERTQuery(line);
        console.log("Update data", result);
        break;
      case "DELETE":
        result = await executeDELETEQuery(line);
        console.log("Rows Deleted:", result);
        break;

      default:
        throw new Error("Unsupported query type");
    }
  } catch (error) {
    console.error("Error:", error.message);
  }

  rl.prompt();
}).on("close", () => {
  console.log("Exiting SQL CLI");
  process.exit(0);
});
