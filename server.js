const {Client} = require('pg');
const fs = require('fs');
const express = require('express');
const path = require('path');
require('dotenv').config();
const app = express();
const PORT = process.env.PORT || 3000;

const connection = new Client({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    port: process.env.DB_PORT,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME
});

// middleware because we used express and the public thing to pass all the public apis
app.use(express.json());
app.use(express.static('public'));

// Postgresql connection
connection.connect().then(() => console.log("DB connected successfully"));

async function importData() {
    try {
        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL,
                age INT NOT NULL,
                address JSONB,
                additional_info JSONB
            );
        `;
        await connection.query(createTableQuery);
        console.log("Table created successfully");

        // if data already exists, then we will not import
        const checkData = await connection.query('SELECT COUNT(*) FROM users');
        if (parseInt(checkData.rows[0].count) > 0) {
            console.log("Data already exists");
            return;
        }

        const csvFile = fs.readFileSync('./env/users_sample.csv', 'utf8');
        const rows = csvFile.toString().split("\n");
        let jsonObject = [];
        const headers = rows[0].split(',');

        for (let i = 1; i < rows.length; i++) {
            const cols = rows[i].split(',');
            let obj = {};
            for (let j = 0; j < headers.length; j++) {
                obj[headers[j].trim()] = cols[j] ? cols[j].trim() : null;
            }
            if (obj['name.firstName']) jsonObject.push(obj);
        }

        const insertQuery = `
            INSERT INTO users (name, age, address, additional_info)
            VALUES ($1, $2, $3, $4)
        `;

        for (const user of jsonObject) {
            const fullName = `${user["name.firstName"]} ${user["name.lastName"]}`.trim();
            const age = parseInt(user.age) || 0;
            const address = {
                line1: user["address.line1"] || null,
                line2: user["address.line2"] || null,
                city: user["address.city"] || null,
                state: user["address.state"] || null
            };
            const additionalInfo = {
                gender: user.gender || null,
                employment: {
                    status: user["employment.status"] || null,
                    company: user["employment.company"] || null
                },
                preferences: {
                    food: user["preferences.food.type"] || null,
                    color: user["preferences.color.favorite"] || null
                }
            };
            await connection.query(insertQuery, [fullName, age, address, additionalInfo]);
        }
        console.log(`Inserted ${jsonObject.length} records successfully`);
    } catch (err) {
        console.error("Import Error:", err.message);
    }
}

// getting all users from database
app.get('/api/users', async (req, res) => {
    try {
        const result = await connection.query('SELECT * FROM users ORDER BY id');
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// getting age distribution by retreiving all the users from the database
app.get('/api/age-distribution', async (req, res) => {
    try {
        const ageQueryResult = await connection.query('SELECT age FROM users');
        let countUnder20 = 0, count20to40 = 0, count40to60 = 0, countOver60 = 0;
        
        for (const row of ageQueryResult.rows) {
            const age = row.age;

            if (age < 20) countUnder20++;
            else if (age < 40) count20to40++;
            else if (age < 60) count40to60++;
            else countOver60++;
        }

        const totalPeople = ageQueryResult.rows.length;

        function calculatePercentage(count, totalCount) {
            const percentage = (count / totalCount) * 100;
            return parseFloat(percentage.toFixed(2));
        }

        res.json({
            total: totalPeople,
            distribution: {
                "Under 20": calculatePercentage(countUnder20, totalPeople),
                "20 - 40": calculatePercentage(count20to40, totalPeople),
                "40 - 60": calculatePercentage(count40to60, totalPeople),
                "Over 60": calculatePercentage(countOver60, totalPeople)
            }
        });

    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// starting server
async function startServer() {
    await importData();
    app.listen(PORT, () => {
        console.log(`Server running on PORT: ${PORT}`);
    });
}

startServer();