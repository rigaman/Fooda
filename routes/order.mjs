import express from 'express';
var router = express.Router();
import {connectClient, endClient, client} from '../datalayer/connection.mjs';
/* GET home page. */
router.post('/order', async function(req, res, next) {
    try {
        await connectClient();
        console.log('Connected to the database');

    } catch (err) {
        console.error('Database connection error:', err);
    } finally {
        try {
            await endClient();
            console.log('Database connection closed');
        } catch (err) {
            console.error('Error closing database connection:', err);
        }
        res.send('Order received successfully');
    }

});

export default router;