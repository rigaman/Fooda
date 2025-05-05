# Fooda Test

## Description
Fooda is going to start rewarding our users for purchases. Every time you buy with Fooda you will get "points". Points can be redeemed for discounts on future purchases. Reward points are calculated on the following schedule:

## Features
- eventService processes event json: 
- Checks if accounts in the event json exist.
- Adds accounts if accounts don't exist in the database  
- Resolves orders with accounts and adds orders to the order table.
- Calculates order rewards based on business logic 
- Generates a report containing each customer with total rewards and average rewards per order.
- Report output should order users by total rewards most to least.
## Installation
Step-by-step instructions to set up the project locally:
1. Clone the repository: `git clone https://github.com/rigaman/Fooda.git`
2. Navigate to the project directory: `cd Fooda`
3. Install dependencies: `npm install` (or equivalent for your tech stack, e.g., `pip install -r requirements.txt` for Python)
4. Run tests `npm test`

## Usage
How to run or use the project:
1. Start the application: `npm start` (or equivalent command, e.g., `python main.py` for Python)
2. Access the app at `http://localhost:3000` (update port or URL as needed).
3. Example usage scenarios or commands.

## Project Structure
A brief overview of the folder structure:
```
├── src/                # Source code
├── __tests__/          # Test files
├── .gitignore          # Ignored files for Git
├── README.md           # This file
├── datalayer/          # application objects responsible for managing data access and storage
├── processor/          # application objects accesesing datalayer and containing business logic
├── service/            # application objects responsible for accsing processor layer and serving json to applications frontend
├── sql/                # contains sql script to create postgres database
└── package.json        # Project metadata and dependencies (or equivalent)
```

## Contributing
We welcome contributions! To contribute:
1. Fork the repository.
2. Create a new branch: `git checkout -b feature-name`
3. Make your changes and commit: `git commit -m "Add feature-name"`
4. Push to your branch: `git push origin feature-name`
5. Open a Pull Request.

Please follow our [Code of Conduct](CODE_OF_CONDUCT.md) and [Contributing Guidelines](CONTRIBUTING.md).

## License
This project is licensed under the [MIT License](LICENSE).

## Contact
For questions or feedback, reach out to:
- Maintainer: [Philip Plekhanov](mailto:p.plekhanov@gmail.com)
- Project Link: [Fooda Test Project Repository](https://github.com/rigaman/Fooda)
