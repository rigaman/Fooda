import { client } from './connection.mjs';

export function getRewardsConfig() {

    return client.query('SELECT Id, StartTime, EndTime, RewardPoints FROM RewardsConfig');

}
