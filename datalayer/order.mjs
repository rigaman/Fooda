import { client } from './connection.mjs';

export async function insertOrders(orders) {

    // Example order object:
    // amount: 8.9,
    // timestamp: "2020-07-01T12:20:00-05:00"
    // account: 1, // Assuming account is an ID, not a name
    // reward: 3, // Assuming reward is a number, default to 0 if not provided

    const values = orders.map(order => `(${order.account}, ${order.amount}, '${order.timestamp}', ${order.reward || 0})`).join(',');
    // Construct the SQL query to insert multiple orders
    const query = `INSERT INTO Order ("Account", "Amount", "Timestamp", "RewardAmount") 
                    VALUES ${values} RETURNING Id, "Account", "Amount", "Timestamp", "RewardAmount"`;

    return client.query(query);
}
export async function getOrdersByCustomerIds(customerIds) {

    const query = `SELECT Id, "Account", "Amount", "Timestamp", "RewardAmount" 
                    FROM Order WHERE "Account" IN (${customerIds.join(',')})`; 

    return client.query(query);
}

