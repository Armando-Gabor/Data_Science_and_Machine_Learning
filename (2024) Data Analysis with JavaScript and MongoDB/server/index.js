// Uvoz potrebnih modula i funkcija
const express = require("express");
const { ConnectDB } = require("./db");
const {
  handleMissingValues,
  calculateStatistics,
  calculateFrequencies,
  splitByMean,
  embedFrequencies,
  embedStatistics,
  extractHighDevMeans,
  createAndUseCompoundIndex,
  exportToJSON,
} = require("./dataOperations");

//Inicijalizacija servera
const app = express();

/*
Povezivanje sa bazom je odrađeno funkcijom u file-u db.js.
Baza i kolekcija se kreiraju i popunjavaju podacima iz CSV file-a (originalni dataset -> AppliancesData.csv) 
ukoliko već ne postoje.
Analiza je odrađena u obliku funkcija u file-u dataOperations.js.
U ovom file-u se samo pokreće server i pozivaju napravljene funkcije za povezivanje s bazom i rješavanje zadataka.
Pokretanje servera "server> npm start"
*/

const initializeServer = async () => {
  try {
    //Konekcija na bazu
    const db = await ConnectDB();

    //Kolekcija s originalnim setom podataka na kojoj se vrše operacije
    const collection = db.collection("AppliancesData");

    //Poziv funkcija
    await handleMissingValues(collection);
    await calculateStatistics(collection);
    await calculateFrequencies(collection);
    await splitByMean(collection);
    await embedFrequencies(collection);
    await embedStatistics(collection);
    await extractHighDevMeans(collection);
    await createAndUseCompoundIndex(collection);
    //Export rezultata
    await exportToJSON(collection);
  } catch (error) {
    console.log("Error: ", error);
  }
};

app.listen(3000, () => {
  initializeServer();
  console.log("Server is running on port 3000");
});
