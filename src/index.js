const parseQuery = require("./queryParser");
const readCSV = require("./csvReader");

async function executeSELECTQuery(query) {
  try {
    const { fields, table } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);

    // Filter the fields based on the query
    return data.map((row) => {
      const filteredRow = {};
      fields.forEach((field) => {
        if (!row.hasOwnProperty(field)) {
          throw new Error(`Field ${field} does not exist in the CSV file.`);
        }
        filteredRow[field] = row[field];
      });
      return filteredRow;
    });
  } catch (error) {
    console.error(`Error executing SELECT query: ${error.message}`);
    throw error;
  }
}

module.exports = executeSELECTQuery;
