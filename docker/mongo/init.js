// Infraestructura solamente: usuarios nuevos; sin colecciones o datos de dominio.
// Se ejecuta una vez al inicializar un volumen vacio, no durante esta fase.
const fs = require('fs');
const readSecret = (variable) => fs.readFileSync(process.env[variable], 'utf8').trim();
const passwords = [
  // El entrypoint oficial puede retirar la variable *_FILE tras leerla.
  fs.readFileSync('/run/secrets/mongo_root_password', 'utf8').trim(),
  readSecret('MONGO_WRITER_PASSWORD_FILE'),
  readSecret('MONGO_READER_PASSWORD_FILE')
];
if (passwords.some(value => value.length < 24) || new Set(passwords).size !== 3) {
  throw new Error('LifeMap requires three distinct passwords of at least 24 characters');
}
if (process.env.MONGO_DATABASE !== 'lifemap') {
  throw new Error('Expected the independent lifemap database');
}
const application = db.getSiblingDB('lifemap');
application.createUser({
  user: process.env.MONGO_WRITER_USERNAME,
  pwd: passwords[1],
  roles: [{role: 'readWrite', db: 'lifemap'}]
});
application.createUser({
  user: process.env.MONGO_READER_USERNAME,
  pwd: passwords[2],
  roles: [{role: 'read', db: 'lifemap'}]
});
