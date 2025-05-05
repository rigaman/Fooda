import { client } from './connection.mjs';

export function getAccounts(accountNames) {

    return client.query('SELECT Id, Name FROM Account WHERE  Name IN ($1)', accountNames);
}
export function insertAccounts(accountNames) {

    // Assuming accountNames is an array of names to be inserted
    const values = accountNames.map(name => `('${name}')`).join(',');
    const query = `INSERT INTO Account (Name) VALUES ${values} RETURNING Id, Name`;
    return client.query(query);
}
