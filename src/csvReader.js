const fs = require("fs");
const csv = require("csv-parser");

const createCsvWriter = require("csv-writer").createObjectCsvWriter;

async function writeCSV(filePath, data) {
  const csvWriter = createCsvWriter({
    path: filePath,
    header: Object.keys(data[0]).map((id) => ({ id, title: id })),
  });

  return csvWriter.writeRecords(data);
}

module.exports = writeCSV;

function readCSV(filePath) {
  const results = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {
        resolve(results);
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}
module.exports = { readCSV, writeCSV };
