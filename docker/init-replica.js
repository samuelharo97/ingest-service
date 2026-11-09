try {
  rs.status();
} catch (error) {
  if (error.code !== 94) throw error;
  rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'mongo:27017' }] });
}

const deadline = Date.now() + 60000;
while (!db.hello().isWritablePrimary && Date.now() < deadline) sleep(100);
if (!db.hello().isWritablePrimary)
  throw new Error('Replica set did not elect a primary');
