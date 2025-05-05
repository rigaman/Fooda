import pg from 'pg';

const { Pool, Client } = pg;
const USER_NAME = process.env.USER_NAME || 'nodeUser';
const USER_PASS = process.env.USER_PASS || 'rigaman';
const HOST = process.env.HOST || 'localhost';
const PORT = process.env.PORT || 5432;
const DB = process.env.DB || 'Fooda';

const options = {
    user: USER_NAME,
    password: USER_PASS,
    host: HOST,
    port: PORT,
    database: DB
};  

//const pool = new Pool(options);
const client = new Client(options);

const openConnecton = () => {
    return client.connect();
}

const closeConnection = () => {
    return client.end();
}

export { openConnecton, closeConnection, client };