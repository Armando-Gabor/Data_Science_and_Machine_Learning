// Uvoz potrebnih modula
const fs = require("fs/promises");
const path = require("path");

// Kontinuirane varijable iz originalnog seta podataka
const CONTINUOUS_VARIABLES = [
  //Temperatura
  "T1",
  "T2",
  "T3",
  "T4",
  "T5",
  "T6",
  "T7",
  "T8",
  "T9",
  "T_out",
  "Tdewpoint",
  //Vlažnost
  "RH_1",
  "RH_2",
  "RH_3",
  "RH_4",
  "RH_5",
  "RH_6",
  "RH_7",
  "RH_8",
  "RH_9",
  "RH_out",
  //Energija
  "Appliances",
  "lights",
  //Ostali podaci
  "Press_mm_hg",
  "Windspeed",
  "Visibility",
  "rv1",
  "rv2",
];

//Kategoričke varijable iz originalnog seta podataka
const CATEGORICAL_VARIABLES = ["date"];

//Pomoćna funkcija za stvaranje kolekcije
const createOrClearCollection = async (db, collectionName) => {
  try {
    const collections = await db.listCollections().toArray();
    if (collections.some((col) => col.name === collectionName)) {
      await db.collection(collectionName).drop();
    }
    return await db.createCollection(collectionName);
  } catch (error) {
    console.log(
      `Error creating/clearing collection ${collectionName}: `,
      error
    );
    throw error;
  }
};

/*
Obrada missing vrijednosti:
*/
const handleMissingValues = async (collection) => {
  try {
    // Logika promjene vrijednosti za kontinuirane varijable koristeći $ifNull
    const continuousUpdate = {};
    CONTINUOUS_VARIABLES.forEach((field) => {
      continuousUpdate[field] = { $ifNull: [`$${field}`, -1] };
    });

    // Update svih dokumenata za kontinuirane varijable uporabom continuousUpdate
    await collection.updateMany({}, [{ $set: continuousUpdate }]);

    // Logika promjene vrijednosti za kategoričke varijable koristeći $ifNull
    const categoricalUpdate = {};
    CATEGORICAL_VARIABLES.forEach((field) => {
      categoricalUpdate[field] = { $ifNull: [`$${field}`, "empty"] };
    });

    // Update svih dokumenata za kategoričke varijable uporabom categoricalUpdate
    await collection.updateMany({}, [{ $set: categoricalUpdate }]);

    console.log("Missing values replaced successfully");
  } catch (error) {
    console.log("Error in handling missing values: ", error);
    throw error;
  }
};

/*
Izračun srednjih vrijednosti, standardnih devijacija i kreacija
novih dokumenata oblika sa vrijednostima, statistika_AppliancesData
*/
const calculateStatistics = async (collection) => {
  try {
    // Dohvaćanje reference na bazu podataka iz kolekcije
    const db = collection.s.db;

    // Stvaranje ili čišćenje kolekcije za pohranu statistika
    const statsCollection = await createOrClearCollection(
      db,
      "statistika_AppliancesData"
    );

    // Iteracija kroz sve kontinuirane varijable definirane u CONTINUOUS_VARIABLES
    for (const variable of CONTINUOUS_VARIABLES) {
      // Agregacija podataka za trenutnu varijablu
      const result = await collection
        .aggregate([
          {
            $group: {
              _id: null, // Grupiranje svih dokumenata u jedan izlazni dokument
              mean: {
                // Računanje srednje vrijednosti
                $avg: {
                  $cond: [
                    { $ne: [`$${variable}`, -1] }, // Provjera da li je vrijednost različita od -1 (oznaka za nedostajuće podatke)
                    `$${variable}`, // Ako je vrijednost valjana, koristi se za izračun
                    "$$REMOVE", // Ako nije valjana, izostavi se iz izračuna
                  ],
                },
              },
              stdDev: {
                // Računanje standardne devijacije
                $stdDevPop: {
                  $cond: [
                    { $ne: [`$${variable}`, -1] }, // Provjera da li je vrijednost različita od -1 (oznaka za nedostajuće podatke)
                    `$${variable}`, // Ako je vrijednost valjana, koristi se za izračun
                    "$$REMOVE", // Ako nije valjana, izostavi se iz izračuna
                  ],
                },
              },
              // Računanje broja valjanih podataka
              nonMissingCount: {
                // Broj ne-nedostajućih (valjanih) vrijednosti
                $sum: { $cond: [{ $ne: [`$${variable}`, -1] }, 1, 0] },
              },
            },
          },
        ])
        .toArray();

      // Ako postoji rezultat agregacije, spremi statistiku u novu kolekciju
      if (result.length > 0) {
        await statsCollection.insertOne({
          Varijabla: variable, // Naziv varijable
          "Srednja vrijednost": result[0].mean, // Srednja vrijednost varijable
          "Standardna devijacija": result[0].stdDev, // Standardna devijacija varijable
          "Broj nomissing elemenata": result[0].nonMissingCount, // Broj valjanih podataka
        });
      }
    }

    // Ispis uspješnog izvođenja funkcije
    console.log(
      "Statistics calculated and stored in new collection successfully"
    );
  } catch (error) {
    // Ispis greške u slučaju neuspješnog izvođenja funkcije
    console.log("Error calculating statistics: ", error);
    throw error;
  }
};

/*
Izračun frekvencije pojavnosti po obilježjima 
varijabli i kreacija novih dokumenata, frekvencija_AppliancesData
*/
const calculateFrequencies = async (collection) => {
  try {
    // Dohvaćanje reference na bazu podataka iz kolekcije
    const db = collection.s.db;

    // Stvaranje ili čišćenje kolekcije za pohranu frekvencija
    const freqCollection = await createOrClearCollection(
      db,
      "frekvencija_AppliancesData"
    );

    // Iteracija kroz sve kategorijske varijable definirane u CATEGORICAL_VARIABLES
    for (const variable of CATEGORICAL_VARIABLES) {
      // Inicijalno kreiranje dokumenta za trenutnu varijablu
      await freqCollection.insertOne({
        Varijabla: variable,
        Pojavnost: {},
      });

      // Dobivanje kursora za iteraciju kroz sve dokumente
      const cursor = collection.find({}, { projection: { [variable]: 1 } });

      // Iteracija kroz sve dokumente u kolekciji
      while (await cursor.hasNext()) {
        const doc = await cursor.next();
        const value = doc[variable] || "empty";

        // Korištenje $inc operatora za povećavanje frekvencije
        await freqCollection.updateOne(
          { Varijabla: variable },
          { $inc: { [`Pojavnost.${value}`]: 1 } }
        );
      }
    }

    console.log(
      "Frequencies calculated and stored in new collection successfully"
    );
  } catch (error) {
    console.log("Error calculating frequencies: ", error);
    throw error;
  }
};

/*
Kreacija dva nova tipa dokumenata u kojima će biti sadržani:
svi elementi <= srednje vrijednosti,
a u drugom dokumentu biti sadržani svi elementi >srednje vrijednosti,
statistika1_AppliancesData i statistika2_AppliancesData
*/
const splitByMean = async (collection) => {
  try {
    // Dohvaćanje reference na bazu podataka iz kolekcije
    const db = collection.s.db;

    // Stvaranje ili čišćenje kolekcija za pohranu podataka
    const stats1Collection = await createOrClearCollection(
      db,
      "statistika1_AppliancesData"
    );
    const stats2Collection = await createOrClearCollection(
      db,
      "statistika2_AppliancesData"
    );

    // Dohvaćanje kolekcije s prethodno izračunatim statistikama
    const statsCollection = db.collection("statistika_AppliancesData");

    // Objekt za pohranu srednjih vrijednosti varijabli
    const meanValues = {};

    // Dohvaćanje svih srednjih vrijednosti iz kolekcije statistika
    await statsCollection.find({}).forEach((stat) => {
      meanValues[stat.Varijabla] = stat["Srednja vrijednost"];
    });

    // Dohvaćanje svih dokumenata iz originalne kolekcije
    const cursor = collection.find({});

    // Iteracija kroz sve dokumente
    while (await cursor.hasNext()) {
      const doc = await cursor.next();
      const lowerEqualDoc = {}; // Dokument za vrijednosti manje ili jednake srednjoj vrijednosti
      const greaterDoc = {}; // Dokument za vrijednosti veće od srednje vrijednosti

      // Prolazak kroz sve kontinuirane varijable
      CONTINUOUS_VARIABLES.forEach((variable) => {
        const value = doc[variable];
        // Ako vrijednost nije -1 (oznaka za nedostajuće podatke)
        if (value !== -1) {
          // Usporedba s srednjom vrijednošću te varijable
          if (value <= meanValues[variable]) {
            lowerEqualDoc[variable] = value;
          } else {
            greaterDoc[variable] = value;
          }
        }
      });

      // Ako postoji barem jedna vrijednost manja ili jednaka srednjoj, spremi u prvu kolekciju
      if (Object.keys(lowerEqualDoc).length > 0) {
        await stats1Collection.insertOne(lowerEqualDoc);
      }
      // Ako postoji barem jedna vrijednost veća od srednje, spremi u drugu kolekciju
      if (Object.keys(greaterDoc).length > 0) {
        await stats2Collection.insertOne(greaterDoc);
      }
    }

    // Ispis uspješnog izvođenja funkcije
    console.log("Data split by mean values successfully");
  } catch (error) {
    // Ispis greške u slučaju neuspješnog izvođenja funkcije
    console.log("Error splitting data by mean: ", error);
    throw error;
  }
};

/*
5. zadatak:
Kreacija novih dokumenata s embedanim vrijednostima
frekvencija pojavnosti emb_AppliancesData
*/
const embedFrequencies = async (collection) => {
  try {
    // Dohvaćanje reference na bazu podataka iz kolekcije
    const db = collection.s.db;

    // Stvaranje ili čišćenje kolekcije za pohranu podataka s ugrađenim frekvencijama
    const embCollection = await createOrClearCollection(
      db,
      "emb_AppliancesData"
    );

    // Dohvaćanje kolekcije s frekvencijama kategorijskih varijabli
    const freqCollection = db.collection("frekvencija_AppliancesData");

    // Objekt za pohranu svih frekvencija
    const frequencies = {};

    // Dohvaćanje svih frekvencija iz kolekcije frekvencija
    await freqCollection.find({}).forEach((doc) => {
      frequencies[doc.Varijabla] = doc.Pojavnost;
    });

    // Dohvaćanje svih dokumenata iz originalne kolekcije
    const cursor = collection.find({});

    // Iteracija kroz sve dokumente
    while (await cursor.hasNext()) {
      const doc = await cursor.next();
      // Kopiranje originalnog dokumenta
      const newDoc = { ...doc };

      // Prolazak kroz sve kategorijske varijable
      CATEGORICAL_VARIABLES.forEach((variable) => {
        const value = doc[variable];
        // Dodavanje frekvencije trenutne vrijednosti varijable u novi dokument
        // Ako vrijednost ne postoji, postavlja se na 0
        newDoc[`${variable}_freq`] = frequencies[variable][value] || 0;
      });

      // Uklanjanje _id polja iz novog dokumenta prije umetanja jer MongoDB automatski generira novi _id
      delete newDoc._id;
      // Umetanje novog dokumenta u kolekciju s ugrađenim frekvencijama
      await embCollection.insertOne(newDoc);
    }

    // Ispis uspješnog izvođenja funkcije
    console.log("Frequencies embedded successfully in new collection");
  } catch (error) {
    // Ispis greške u slučaju neuspješnog izvođenja funkcije
    console.log("Error embedding frequencies: ", error);
    throw error;
  }
};

/*
Kreacija novih dokumenata s embedanim statističkim
podacima o kontinuiranim varijablama emb_AppliancesData
*/
const embedStatistics = async (collection) => {
  try {
    // Dohvaćanje reference na bazu podataka iz kolekcije
    const db = collection.s.db;

    // Stvaranje ili čišćenje kolekcije za pohranu podataka s ugrađenim statistikama
    const emb2Collection = await createOrClearCollection(
      db,
      "emb2_AppliancesData"
    );

    // Dohvaćanje kolekcije s prethodno izračunatim statistikama
    const statsCollection = db.collection("statistika_AppliancesData");

    // Objekt za pohranu svih statistika
    const statistics = {};

    // Dohvaćanje svih statistika iz kolekcije statistika
    await statsCollection.find({}).forEach((doc) => {
      statistics[doc.Varijabla] = {
        "Srednja vrijednost": doc["Srednja vrijednost"],
        "Standardna devijacija": doc["Standardna devijacija"],
        "Broj nomissing elemenata": doc["Broj nomissing elemenata"],
      };
    });

    // Dohvaćanje svih dokumenata iz originalne kolekcije
    const cursor = collection.find({});
    let counter = 0;

    // Iteracija kroz sve dokumente
    while (await cursor.hasNext()) {
      const doc = await cursor.next();
      // Kopiranje originalnog dokumenta
      const newDoc = { ...doc };

      // Prolazak kroz sve kontinuirane varijable
      CONTINUOUS_VARIABLES.forEach((variable) => {
        // Dodavanje statistika varijable u novi dokument
        newDoc[`${variable}_stats`] = statistics[variable];
      });

      // Uklanjanje _id polja iz novog dokumenta prije umetanja jer MongoDB automatski generira novi _id
      delete newDoc._id;
      // Umetanje novog dokumenta u kolekciju s ugrađenim statistikama
      await emb2Collection.insertOne(newDoc);
      counter++;
    }

    // Ispis uspješnog izvođenja funkcije
    console.log("Statistics embedded successfully in new collection");
  } catch (error) {
    // Ispis greške u slučaju neuspješnog izvođenja funkcije
    console.log("Error embedding statistics: ", error);
    throw error;
  }
};

/*
Izvlačenje srednjih vrijednosti varijabli
čija je standardna devijacija 10% > srednje vrijednosti
*/
const extractHighDevMeans = async (collection) => {
  try {
    // Dohvaćanje reference na bazu podataka iz kolekcije
    const db = collection.s.db;

    // Dohvaćanje kolekcije gdje su ugrađene statistike
    const emb2Collection = db.collection("emb2_AppliancesData");

    // Dohvaćanje svih dokumenata iz kolekcije emb2_AppliancesData
    const emb2Items = await emb2Collection.find().toArray();

    // Obrada svakog dokumenta
    for (const emb2Item of emb2Items) {
      for (const variable of CONTINUOUS_VARIABLES) {
        // Dohvaćanje statistika za trenutnu varijablu
        const stats = emb2Item[`${variable}_stats`];

        if (stats) {
          const meanValue = stats["Srednja vrijednost"]; // Srednja vrijednost
          const stdDev = stats["Standardna devijacija"]; // Standardna devijacija

          // Izračun omjera standardne devijacije u odnosu na srednju vrijednost u postocima
          const result = Math.round((stdDev / meanValue) * 100);

          // Ako je omjer veći od 10%, ažuriraj dokument s novim poljem
          if (result > 10) {
            const updateField = `${variable}_stats.stddev_>_mean_u_%`;

            // Ažuriranje dokumenta u bazi podataka
            await emb2Collection.updateOne(
              { _id: emb2Item._id }, // Pronađi dokument po _id
              {
                $set: { [updateField]: result }, // Postavi novo polje s izračunatom vrijednošću
              }
            );
          }
        }
      }
    }

    // Ispis uspješnog izvođenja funkcije
    console.log("High deviation means extracted and updated successfully");
  } catch (error) {
    // Ispis greške u slučaju neuspješnog izvođenja funkcije
    console.log("Error extracting high deviation means: ", error);
    throw error;
  }
};

/*
Kreacija složenog indeksa na originalnoj tablici
*/
const createAndUseCompoundIndex = async (collection) => {
  try {
    // Dohvaćanje reference na bazu podataka iz kolekcije
    const db = collection.s.db;

    // Stvaranje ili čišćenje kolekcije za pohranu rezultata upita
    const indexQueryCollection = await createOrClearCollection(
      db,
      "index_query_results"
    );

    // Kreiranje složenog indeksa na poljima 'date' (padajući) i 'T_out' (rastući)
    await collection.createIndex(
      { date: -1, T_out: 1 },
      { name: "idx_date_temp" }
    );

    // Izvođenje upita koristeći novi indeks
    const results = await collection
      .find({
        date: { $gte: "2016-01-01", $lte: "2016-12-31" }, // Datum između 1. siječnja i 31. prosinca 2016.
        T_out: { $gt: 20 }, // Temperatura vani veća od 20
      })
      .sort({ date: -1, T_out: 1 }) // Sortiranje po datumu (date, padajući) i temperaturi (T_out, rastući)
      .toArray();

    // Umetanje rezultata upita u novu kolekciju bez _id polja
    for (const doc of results) {
      delete doc._id; // Uklanjanje _id polja da bi MongoDB automatski generirao novi
      await indexQueryCollection.insertOne(doc);
    }

    // Ispis uspješnog izvođenja funkcije
    console.log("Index query results stored in new collection");
  } catch (error) {
    // Ispis greške u slučaju neuspješnog izvođenja funkcije
    console.log("Error in index creation and query: ", error);
    throw error;
  }
};

/*
Dodatna funkcija za export svih novo stvorenih dokumenata kao JSON file-ova
*/
const exportToJSON = async (collection) => {
  try {
    // Dohvaćanje reference na bazu podataka iz kolekcije
    const db = collection.s.db;

    // Lista kolekcija koje treba izvesti u JSON format
    const collectionsToExport = [
      "statistika_AppliancesData",
      "statistika1_AppliancesData",
      "statistika2_AppliancesData",
      "frekvencija_AppliancesData",
      "emb_AppliancesData",
      "emb2_AppliancesData",
      "index_query_results",
    ];

    // Definiranje direktorija za izvoz
    const exportDir = path.join(__dirname, "..", "rezultati");
    // Stvaranje direktorija ako ne postoji
    await fs.mkdir(exportDir, { recursive: true });

    // Iteracija kroz sve kolekcije koje treba izvesti
    for (const collectionName of collectionsToExport) {
      // Dohvaćanje reference na trenutnu kolekciju
      const targetCollection = db.collection(collectionName);

      // Dohvaćanje svih dokumenata iz kolekcije, isključujući polje _id
      const documents = await targetCollection
        .find({}, { projection: { _id: 0 } })
        .toArray();

      // Stvaranje putanje do datoteke za izvoz
      const filePath = path.join(exportDir, `${collectionName}.json`);
      // Pisanje dokumenata u JSON datoteku s formatiranjem (2 razmaka za indentaciju)
      await fs.writeFile(filePath, JSON.stringify(documents, null, 2));
      console.log(`Exported ${collectionName} to JSON`);
    }

    // Ispis uspješnog izvođenja funkcije
    console.log("All collections exported to JSON successfully");
  } catch (error) {
    // Ispis greške u slučaju neuspješnog izvođenja funkcije
    console.log("Error exporting to JSON: ", error);
    throw error;
  }
};

//Export funkcija za rješavanje zadataka
module.exports = {
  handleMissingValues,
  calculateStatistics,
  calculateFrequencies,
  splitByMean,
  embedFrequencies,
  embedStatistics,
  extractHighDevMeans,
  createAndUseCompoundIndex,
  exportToJSON,
};
