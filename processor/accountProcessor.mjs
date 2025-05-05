import { getAccounts, insertAccounts } from '../datalayer/account.mjs';

export async function processAccounts(jsonData) {

    let accountMap = new Map();

    const events = jsonData && jsonData.events ? jsonData.events : [];

    if (!events || !events.length) {
        throw new Error('Name is required');
    }
    const accountNames = new Set();
    for (const event of events) {
        if (event.name && event.name.length) {
            accountNames.add(event.name);
        }
    }

    if (accountNames.size !== 0) {
        //create map of accounts
        const existingAccounts =  await getAccounts(Array.from(accountNames));

        if (existingAccounts || existingAccounts.rows) {
    
            //add existing accounts to the map
            for (const account of existingAccounts.rows) {
                accountMap.set(account.Name, account);
            }
        }
    
        //create new accounts array
        //filter out accounts that already exist in the database
        const newAccounts = Array.from(accountNames).filter(name => !accountMap.has(name));
        if (newAccounts.length === 0) {
            return accountMap; // No new accounts to insert, return the existing map
        }
        const insertedAccounts = await insertAccounts(newAccounts);
        //add inserted accounts to the map
        for (const account of insertedAccounts.rows) {
            accountMap.set(account.Name, account);
        }
    }
    return accountMap;

}