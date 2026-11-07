// Prueba sintetica exclusiva de infraestructura; no utiliza colecciones Yelp.
(() => {
  const fsProbe = require('fs');
  const mode = process.env.LIFEMAP_PROBE_MODE;
  const runId = process.env.LIFEMAP_PROBE_ID;
  if (!['create', 'verify'].includes(mode) || !/^phase3-[a-f0-9]{32}$/.test(runId || '')) {
    throw new Error('Invalid infrastructure probe parameters');
  }
  const secret = name => fsProbe.readFileSync('/run/secrets/' + name, 'utf8').trim();
  const check = (value, message) => { if (!value) throw new Error(message); };
  const login = (name, password, authDb = 'lifemap') => {
    const connection = new Mongo('mongodb://127.0.0.1:27017');
    const result = connection.getDB(authDb).auth(name, password);
    check(result && result.ok !== 0, 'Authentication rejected');
    return connection.getDB('lifemap');
  };
  const reader = login('lifemap_reader', secret('mongo_reader_password'));
  const collection = '__infra_phase3_probe';
  const proof = {run_id: runId, mode: mode, collection};
  if (mode === 'create') {
    const administrator = login('lifemap_admin', secret('mongo_root_password'), 'admin');
    check(administrator.runCommand({ping: 1}).ok === 1, 'Authenticated ping failed');
    const users = administrator.runCommand({usersInfo: 1, showCredentials: false}).users;
    for (const [name, role] of [['lifemap_reader', 'read'], ['lifemap_writer', 'readWrite']]) {
      const entry = users.find(user => user.user === name);
      check(entry && entry.roles.length === 1 && entry.roles[0].role === role && entry.roles[0].db === 'lifemap', 'Unexpected role grant');
    }
    const writer = login('lifemap_writer', secret('mongo_writer_password'));
    writer.getCollection(collection).insertOne({_id: runId, kind: 'synthetic-infrastructure-probe', value: 42});
    const denied = operation => {
      try {
        const result = operation();
        if (result && result.ok === 0 && result.code === 13) return;
      } catch (error) {
        if (error.code === 13) return;
        throw error;
      }
      throw new Error('Operation should have been unauthorized');
    };
    denied(() => reader.getCollection(collection).insertOne({_id: runId + '-forbidden'}));
    denied(() => writer.getSiblingDB('admin').runCommand({usersInfo: 1}));
    const anonymous = new Mongo('mongodb://127.0.0.1:27017').getDB('lifemap');
    denied(() => anonymous.getCollection(collection).findOne({_id: runId}));
    let badPasswordRejected = false;
    try {
      const connection = new Mongo('mongodb://127.0.0.1:27017');
      const result = connection.getDB('lifemap').auth('lifemap_reader', 'invalid-synthetic-probe-password');
      badPasswordRejected = !result || result.ok === 0;
    } catch (error) {
      if (error.code === 18) badPasswordRejected = true;
      else throw error;
    }
    check(badPasswordRejected, 'Invalid password unexpectedly accepted');
    proof.authenticated_ping = true;
    proof.users_and_roles = true;
    proof.writer_insert = true;
    proof.reader_write_denied = true;
    proof.writer_admin_denied = true;
    proof.anonymous_read_denied = true;
    proof.bad_password_rejected = true;
  }
  const record = reader.getCollection(collection).findOne({_id: runId});
  check(record && record.value === 42 && record.kind === 'synthetic-infrastructure-probe', 'Synthetic record missing or altered');
  proof.reader_read = true;
  proof.document_count_for_run = reader.getCollection(collection).countDocuments({_id: runId});
  print('LIFEMAP_INFRA_PROBE=' + JSON.stringify(proof));
})();
