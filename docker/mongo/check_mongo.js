// Solo autenticacion y ping; no escribe documentos.
const fs = require('fs');
const connection = new Mongo('mongodb://127.0.0.1:27017');
const application = connection.getDB('lifemap');
const password = fs.readFileSync(process.env.MONGO_READER_PASSWORD_FILE, 'utf8').trim();
if (!application.auth(process.env.MONGO_READER_USERNAME, password)) quit(1);
if (application.runCommand({ping: 1}).ok !== 1) quit(2);
