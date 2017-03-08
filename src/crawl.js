const axios = require('axios');
const pageLimit = 100000;
const request = require('request');
const fs = require('fs');
const dataDir = '../data/';
const retries = 5;
const retryDelay = 5;
let startPage = 1;

if (process.argv.length === 3) { 
  startPage = process.argv[2];
}

console.log('nrel-dump');

function wait(sec) {
  return new Promise(resolve => setTimeout(resolve, sec * 1000));
}

function getFiles(files, uuid) {
  return Promise.all(files.map(file =>
    new Promise((resolve, reject) => {
      // console.log('fetching', file.filename);
      const fstream = fs.createWriteStream(`${dataDir}${uuid}/${file.filename}`);
      function req(attempt) {
        // console.log(attempt, file.filename);
        request(file.url, async (err) => {
          if (err) {
            // console.error('stream err', err);
            if (attempt >= retries) {
              reject(err);
            } else {
              await wait(retryDelay * attempt);
              req(attempt + 1);
            }
          }
        }).pipe(fstream).on('finish', resolve);
      }
      req(1); 
    })
  ));
}

function writeJson(row) {
  const uuid = row.component ? row.component.uuid : row.measure.uuid;
  // console.log('uuid', uuid);
  if (!fs.existsSync(dataDir + uuid)) {
    fs.mkdirSync(dataDir + uuid);
  }
  fs.writeFileSync(
    `${dataDir}${uuid}/${uuid}.json`,
    JSON.stringify(row, null, 2),
    'utf-8'
  );
}

async function processPage(page) {
  let attempt = 1;
  let error = null;
  let data = null;

  do {
    console.log('page', page, 'attempt', attempt);
    try {
      data = (await axios.get(
        `http://bcl.nrel.gov/api/search/%20.json?page=${page}&show_rows=100&api_version=2.0`
      )).data.result;
      await Promise.all(data.map((row) => {
        writeJson(row);
        const d = row.component || row.measure;
        if (!d.files) {
          return null;
        }
        return getFiles(d.files.file, d.uuid);
      }));
      return data;
    } catch (err) {
      console.error(err);
      error = err;
    }
    await wait(retryDelay * attempt);
    attempt++;
  } while (attempt <= retries);
  
  throw new Error(error);
}

async function crawl() {
  let page = startPage;
  let data = null;
  try {
    do {
      data = await processPage(page);
      page++;
    } while (data.length && page < pageLimit)
  } catch (err) {
    console.error(err);
  }
}

crawl();