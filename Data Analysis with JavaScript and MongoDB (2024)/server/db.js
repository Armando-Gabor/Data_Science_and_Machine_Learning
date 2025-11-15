// Uvoz potrebnih modula
const { MongoClient } = require("mongodb");
const fs = require("fs");
const csv = require("csv-parser");
const path = require("path");

// MongoDB podaci za povezivanje
const url = "mongodb://127.0.0.1:27017";
const client = new MongoClient(url);
const dbName = "AppliancesDataAnalysis";

// Funkcija za povezivanje na bazu
const ConnectDB = async () => {
  try {
    // Povezivanje na klijent
    await client.connect();
    console.log("Connected to MongoDB");

    // Provjera postojanja baze
    const adminDb = client.db("admin");
    const dbList = await adminDb.admin().listDatabases();
    const dbExists = dbList.databases.some((db) => db.name === dbName);

    // Kreiranje baze i kolekcije ako ne postoje
    const db = client.db(dbName);
    if (!dbExists) {
      console.log("Creating new database...");
      // Provjera postojanja kolekcije
      const collections = await db.listCollections().toArray();
      const collectionExists = collections.some(
        (col) => col.name === "AppliancesData"
      );

      if (!collectionExists) {
        console.log("Creating collection and importing data...");
        // Kreiranje kolekcije te automatski i baze
        const collection = await db.createCollection("AppliancesData");

        // Import podataka iz CSV file-a
        const results = [];
        const csvPath = path.join(__dirname, "..", "AppliancesData.csv");
        await new Promise((resolve, reject) => {
          fs.createReadStream(csvPath)
            .pipe(csv())
            .on("data", (data) => {
              Object.keys(data).forEach((key) => {
                if (key !== "date" && !isNaN(data[key])) {
                  data[key] = parseFloat(data[key]);
                }
              });
              results.push(data);
            })
            .on("end", async () => {
              try {
                await collection.insertMany(results);
                console.log(`Imported ${results.length} documents`);
                resolve();
              } catch (err) {
                reject(err);
              }
            })
            .on("error", (error) => reject(error));
        });
      }
    } else {
      // Provjera postojanja kolekcije ako baza već postoji
      const collections = await db.listCollections().toArray();
      const collectionExists = collections.some(
        (col) => col.name === "AppliancesData"
      );

      if (!collectionExists) {
        console.log(
          "Database exists, creating collection and importing data..."
        );
        // Kod za kreiranje kolekcije i import podataka (isti kao gore)
        const collection = await db.createCollection("AppliancesData");

        // Import podataka iz CSV file-a
        const results = [];
        const csvPath = path.join(__dirname, "..", "AppliancesData.csv");
        await new Promise((resolve, reject) => {
          fs.createReadStream(csvPath)
            .pipe(csv())
            .on("data", (data) => {
              Object.keys(data).forEach((key) => {
                if (key !== "date" && !isNaN(data[key])) {
                  data[key] = parseFloat(data[key]);
                }
              });
              results.push(data);
            })
            .on("end", async () => {
              try {
                await collection.insertMany(results);
                console.log(`Imported ${results.length} documents`);
                resolve();
              } catch (err) {
                reject(err);
              }
            })
            .on("error", (error) => reject(error));
        });
      }
      //Baza i kolekcija već postoje
      else {
        console.log("Database and collection already exist");
      }
    }

    return db;
  } catch (error) {
    console.log("Error: ", error);
    throw error;
  }
};

module.exports = { ConnectDB, client };
