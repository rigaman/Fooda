import express from 'express';
var router = express.Router();
import {connectClient, endClient, client} from '../datalayer/connection.mjs';
/* GET home page. */
router.get('/', async function(req, res, next) {
  try {
    await openConnection();
    console.log('Connected to the database');

  } catch (err) {
    console.error('Database connection error:', err);
  } finally {
    try {
  
      await closeConnection();
      console.log('Database connection closed');
    } catch (err) {
      console.error('Error closing database connection:', err);
    }
  }
  res.render('index', { title: 'Express', jsonData: {name: 'John Doe', age: 30, city: 'New York'} });
});

export default router;
