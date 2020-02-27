const Pryv = require('pryv');
const fs = require('fs');
const slugify = require('slugify');



if (process.argv.length <= 2) {
  console.log("Usage: " + __filename + "  file code");
  process.exit(-1);
}

const code = process.argv[2];

console.log('Going for: ' + code);


const settings = {
  service_info: 'https://reg.pryv.me/service/info',
  username: code,
  password: code,
  file: 'data/' + code + '.csv'
}

async function signIn() {
  const conn = await (new Pryv.Service(settings.service_info)).login(
    settings.username,
    settings.password,
    'google-analytics-loader'
  );
  console.log(conn);
  return conn;
}

function getStreamId(streamName) {
  return slugify(streamName.replace('(', '').replace(')', '').split('.').join('').replace(':', '').replace('’', '').toLowerCase());
}

function addStream(context, name, id, parentId) {
  if (context.streamList.includes(id)) return;
  context.batch.push(
    {
      method: 'streams.create',
      params: {
        id: id,
        parentId: parentId,
        name: name
      }
    });
  context.streamList.push(id);
}

function checkStreams(context, country, source, medium) {
  addStream(context, 'Google Analytics', 'ga', null);

  const countryId = getStreamId(country);
  addStream(context, country, countryId, 'ga');

  const sourceId = countryId + '_' + getStreamId(source);
  addStream(context, source, sourceId, countryId);

  const mediumId = sourceId + '_' + getStreamId(medium);
  addStream(context, medium, mediumId, sourceId);
  return mediumId;
}

async function loadFile(fileName) {
  const fileContent = fs.readFileSync(fileName).toString('utf8');
  const lines = fileContent.split('\n');

  const context = {
    batch: [],
    streamList: []
  }

  let ignoreFirst = true;
  lines.forEach(function (line) {
    if (ignoreFirst) { ignoreFirst = false; return };
    const [country, dateStr, source, medium, duration, count] = line.split(';');
    const streamId = checkStreams(context, country, source, medium);

    const date = new Date('01 Jan 1970 00:00:00 GMT');
    const [day, month, year] = dateStr.split('.');
    date.setFullYear(year);
    date.setMonth(month - 1);
    date.setDate(day);
    [
      { type: 'count/generic', content: count },
      { type: 'time/s', content: duration }
    ].forEach(function (event) {
      context.batch.push(
        {
          method: 'events.create',
          params: {
            streamId: streamId,
            time: date.getTime() / 1000,
            content: event.content,
            type: event.type
          }
        });
    });
  });
  return context.batch;
}

async function myFlow() {
  const conn = await signIn();
  const batch = await loadFile(settings.file);

  conn.options.chunkSize = 50;
  const start = (new Date()).getTime() / 1000;
  let lastDate = start;
  let lastPos = 0;
  const result = await conn.api(batch, function (percent, res) {
    const now = (new Date()).getTime() / 1000;
    const eSec = Math.round(res.length / (now - start));
    const nowESec = Math.round((res.length - lastPos) / (now - lastDate));
    lastPos = res.length;
    lastDate = now;
    console.log('> ' + percent + '%  -> ' + res.length + '  avg:' + eSec + ' call/s  last:' + nowESec + ' call/s');
    res.forEach(function (item) {
      if (item.error) {
        console.log(JSON.stringify(item, null, 2));
      }
    })
  });
  console.log("Loaded ")
}

myFlow();