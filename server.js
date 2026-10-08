const express = require('express');
const path = require('path');
const { Pool } = require('pg');
const ZKLib = require('node-zklib'); // Library សម្រាប់ភ្ជាប់ជាមួយម៉ាស៊ីនស្កេន ZKTeco
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname)));

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});

// មុខងារបង្កើត Table និងបញ្ចូលទិន្នន័យគំរូទាំងអស់
async function initializeDatabase() {
    try {
        const client = await pool.connect();
        
        // ១. តារាង Workflow សម្រាប់គ្រប់ផ្នែក
        await client.query(`
            CREATE TABLE IF NOT EXISTS department_workflows (
                id SERIAL PRIMARY KEY,
                department_id VARCHAR(50) NOT NULL,
                step_no INT NOT NULL,
                workflow_stage VARCHAR(255) NOT NULL,
                action_description TEXT NOT NULL
            );
        `);

        // ២. តារាងព័ត៌មានបុគ្គលិក
        await client.query(`
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                employee_code VARCHAR(50) UNIQUE NOT NULL,
                full_name VARCHAR(150) NOT NULL,
                department VARCHAR(100) NOT NULL,
                position VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);

        // ៣. តារាងកត់ត្រាម៉ោងចេញ/ចូលប្រចាំថ្ងៃ
        await client.query(`
            CREATE TABLE IF NOT EXISTS attendance_logs (
                id SERIAL PRIMARY KEY,
                employee_code VARCHAR(50) NOT NULL,
                work_date DATE NOT NULL,
                check_in_time TIMESTAMP,
                check_out_time TIMESTAMP,
                status VARCHAR(50) DEFAULT 'Present',
                note TEXT
            );
        `);

        // ៤. តារាងគ្រប់គ្រងទ្រព្យសម្បត្តិ និងសារពើភណ្ឌ (Assets & Inventory)
        await client.query(`
            CREATE TABLE IF NOT EXISTS inventory_assets (
                id SERIAL PRIMARY KEY,
                item_code VARCHAR(50) UNIQUE NOT NULL,
                item_name VARCHAR(150) NOT NULL,
                category VARCHAR(100) NOT NULL,
                quantity INT NOT NULL DEFAULT 0,
                unit VARCHAR(50) NOT NULL,
                status VARCHAR(50) DEFAULT 'Available',
                holder_or_location VARCHAR(150),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);

        // ៥. តារាងជ្រើសរើសបុគ្គលិក (Job Vacancies) - [បន្ថែមថ្មី]
        await client.query(`
            CREATE TABLE IF NOT EXISTS job_vacancies (
                id SERIAL PRIMARY KEY,
                title VARCHAR(150) NOT NULL,
                dept VARCHAR(100) NOT NULL,
                qty VARCHAR(50) NOT NULL,
                status VARCHAR(50) DEFAULT 'កំពុងស្វែងរក',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);

        // ៦. តារាងផែនការបណ្តុះបណ្តាល (Training Schedules) - [បន្ថែមថ្មី]
        await client.query(`
            CREATE TABLE IF NOT EXISTS training_schedules (
                id SERIAL PRIMARY KEY,
                topic VARCHAR(200) NOT NULL,
                training_date DATE NOT NULL,
                status VARCHAR(100) DEFAULT 'គ្រោងទុក (Scheduled)',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);

        // បញ្ចូលទិន្នន័យគំរូសម្រាប់ Workflow វត្តមាន (បើមិនទាន់មាន)
        const checkData = await client.query("SELECT COUNT(*) FROM department_workflows WHERE department_id = 'HR_ATTENDANCE'");
        if (parseInt(checkData.rows[0].count) === 0) {
            await client.query(`
                INSERT INTO department_workflows (department_id, step_no, workflow_stage, action_description) 
                VALUES 
                ('HR_ATTENDANCE', 1, 'ស្កេនវត្តមានពេលព្រឹក (Morning Check-in)', 'បុគ្គលិកធ្វើការស្កេនម្រាមដៃ ឬកាតវត្តមានចូលធ្វើការពេលព្រឹកតាមម៉ោងកំណត់។'),
                ('HR_ATTENDANCE', 2, 'ពិនិត្យភាពយឺតយ៉ាវ (Late Tracking)', 'ផ្នែករដ្ឋបាលធ្វើការផ្ទៀងផ្ទាត់ និងកត់ត្រាឈ្មោះបុគ្គលិកមកយឺត ឬអវត្តមានពេលព្រឹក។'),
                ('HR_ATTENDANCE', 3, 'សម្រាកថ្ងៃត្រង់ ចេញ/ចូល (Lunch Break)', 'គ្រប់គ្រងម៉ោងចេញ និងចូលសម្រាកថ្ងៃត្រង់របស់បុគ្គលិកប្រចាំថ្ងៃ។'),
                ('HR_ATTENDANCE', 4, 'ស្កេនវត្តមានពេលល្ងាច (Evening Check-out)', 'បុគ្គលិកស្កេនស្ដុបម៉ោងចេញពីការងារពេលល្ងាច និងសង្ខេបិន្នន័យម៉ោងការងារ។');
            `);
            console.log('-> បានបញ្ចូលទិន្នន័យគំរូស្តីពី វត្តមាន ម៉ោងចេញ/ចូល ក្នុង Database ដោយជោគជ័យ!');
        }

        client.release();
        console.log('-> Database Tables ទាំងអស់បានរៀបចំរួចរាល់!');
    } catch (err) {
        console.error('Database Initialization Error:', err);
    }
}

// ==========================================
// មុខងារភ្ជាប់ជាមួយម៉ាស៊ីនស្កេនវត្តមាន (Auto Connect ZKTeco)
// ==========================================
const zkInstance = new ZKLib('192.168.1.201', 4370, 10000, 4000);

async function connectToBiometricMachine() {
    try {
        await zkInstance.createSocket();
        console.log('-> បានតភ្ជាប់ជាមួយម៉ាស៊ីនស្កេនវត្តមាន (ZKTeco) ដោយជោគជ័យ!');

        zkInstance.getRealTimeLogs(async (data) => {
            const employee_code = data.pin;
            const scanTime = new Date(data.time);
            const today = scanTime.toISOString().split('T')[0];

            try {
                const checkQuery = `SELECT * FROM attendance_logs WHERE employee_code = $1 AND work_date = $2`;
                const existing = await pool.query(checkQuery, [employee_code, today]);

                if (existing.rows.length === 0) {
                    const insertQuery = `
                        INSERT INTO attendance_logs (employee_code, work_date, check_in_time, status) 
                        VALUES ($1, $2, $3, 'Present');
                    `;
                    await pool.query(insertQuery, [employee_code, today, scanTime]);
                } else if (!existing.rows[0].check_out_time) {
                    const updateQuery = `
                        UPDATE attendance_logs 
                        SET check_out_time = $1 
                        WHERE employee_code = $2 AND work_date = $3;
                    `;
                    await pool.query(updateQuery, [scanTime, employee_code, today]);
                }
            } catch (dbErr) {
                console.error('កំហុសក្នុងការកត់ត្រាទិន្នន័យពីម៉ាស៊ីនស្កេនចូល Database:', dbErr);
            }
        });

    } catch (e) {
        console.log('មិនអាចតភ្ជាប់ទៅកាន់ម៉ាស៊ីនស្កេនបានទេ:', e.message);
    }
}

// ==========================================
// HTML Routes សម្រាប់បើកទំព័រនីមួយៗ
// ==========================================
app.get('/views/hr-in-out.html', (req, res) => {
    res.sendFile(path.join(__dirname, 'views', 'hr-in-out.html'));
});

app.get('/views/hr-asset.html', (req, res) => {
    res.sendFile(path.join(__dirname, 'views', 'hr-asset.html'));
});

app.get('/views/hr-management.html', (req, res) => {
    res.sendFile(path.join(__dirname, 'views', 'hr-management.html'));
});

// ==========================================
// API សម្រាប់ Attendance & Inventory
// ==========================================
app.get('/api/employees', async (req, res) => {
    try {
        const result = await pool.query("SELECT * FROM employees ORDER BY id DESC");
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: 'មិនអាចទាញយកទិន្នន័យបុគ្គលិកបានទេ!' });
    }
});

app.post('/api/employees', async (req, res) => {
    const { employee_code, full_name, department, position } = req.body;
    try {
        const query = `INSERT INTO employees (employee_code, full_name, department, position) VALUES ($1, $2, $3, $4) RETURNING *;`;
        const result = await pool.query(query, [employee_code, full_name, department, position]);
        res.status(201).json({ message: 'បានបន្ថែមបុគ្គលិកដោយជោគជ័យ!', data: result.rows[0] });
    } catch (err) {
        res.status(500).json({ error: 'កំហុសក្នុងការបញ្ចូលបុគ្គលិក (អាចជាន់កូដ)' });
    }
});

app.get('/api/attendance/logs', async (req, res) => {
    try {
        let { from, to, search } = req.query;
        let query = `
            SELECT a.*, e.full_name, e.department 
            FROM attendance_logs a 
            JOIN employees e ON a.employee_code = e.employee_code 
            WHERE 1=1
        `;
        let params = [];
        let paramIndex = 1;

        if (from && to) {
            query += ` AND a.work_date BETWEEN $${paramIndex} AND $${paramIndex + 1}`;
            params.push(from, to);
            paramIndex += 2;
        }

        if (search) {
            query += ` AND (e.full_name ILIKE $${paramIndex} OR a.employee_code ILIKE $${paramIndex})`;
            params.push(`%${search}%`);
            paramIndex += 1;
        }

        query += ` ORDER BY a.work_date DESC, a.id DESC;`;
        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: 'មិនអាចទាញយកទិន្នន័យវត្តមានបានទេ!' });
    }
});

app.get('/api/inventory', async (req, res) => {
    try {
        let { search, category } = req.query;
        let query = "SELECT * FROM inventory_assets WHERE 1=1";
        let params = [];
        let paramIndex = 1;

        if (search) {
            query += ` AND (item_name ILIKE $${paramIndex} OR item_code ILIKE $${paramIndex} OR holder_or_location ILIKE $${paramIndex})`;
            params.push(`%${search}%`);
            paramIndex += 1;
        }

        if (category) {
            query += ` AND category = $${paramIndex}`;
            params.push(category);
            paramIndex += 1;
        }

        query += " ORDER BY id DESC;";
        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: 'មិនអាចទាញយកទិន្នន័យស្តុកបានទេ!' });
    }
});

app.post('/api/inventory', async (req, res) => {
    const { item_code, item_name, category, quantity, unit, status, holder_or_location } = req.body;
    try {
        const query = `
            INSERT INTO inventory_assets (item_code, item_name, category, quantity, unit, status, holder_or_location) 
            VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *;
        `;
        const result = await pool.query(query, [item_code, item_name, category, quantity, unit, status, holder_or_location]);
        res.status(201).json({ message: 'បានបន្ថែមទ្រព្យសម្បត្តិដោយជោគជ័យ!', data: result.rows[0] });
    } catch (err) {
        res.status(500).json({ error: 'កំហុសក្នុងការបញ្ចូលទិន្នន័យ (អាចជាន់កូដសម្ភារៈ)' });
    }
});

// ==========================================
// API សម្រាប់ HR Recruitment & Training (បន្ថែមថ្មី)
// ==========================================
app.get('/api/jobs', async (req, res) => {
    try {
        const result = await pool.query("SELECT * FROM job_vacancies ORDER BY id DESC");
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: 'មិនអាចទាញយកទិន្នន័យតំណែងការងារបានទេ!' });
    }
});

app.post('/api/jobs', async (req, res) => {
    const { title, dept, qty } = req.body;
    try {
        const query = `INSERT INTO job_vacancies (title, dept, qty, status) VALUES ($1, $2, $3, 'កំពុងស្វែងរក') RETURNING *;`;
        const result = await pool.query(query, [title, dept, qty + " នាក់"]);
        res.status(201).json({ success: true, message: 'បានរក្សាទុកតំណែងការងារដោយជោគជ័យ!', data: result.rows[0] });
    } catch (err) {
        res.status(500).json({ error: 'កំហុសក្នុងការបញ្ចូលតំណែងការងារចូល Database' });
    }
});

app.get('/api/trainings', async (req, res) => {
    try {
        const result = await pool.query("SELECT * FROM training_schedules ORDER BY id DESC");
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: 'មិនអាចទាញយកទិន្នន័យបណ្តុះបណ្តាលបានទេ!' });
    }
});

app.post('/api/trainings', async (req, res) => {
    const { topic, training_date } = req.body;
    try {
        const query = `INSERT INTO training_schedules (topic, training_date, status) VALUES ($1, $2, 'គ្រោងទុក (Scheduled)') RETURNING *;`;
        const result = await pool.query(query, [topic, training_date]);
        res.status(201).json({ success: true, message: 'បានកត់ត្រាការបណ្តុះបណ្តាលដោយជោគជ័យ!', data: result.rows[0] });
    } catch (err) {
        res.status(500).json({ error: 'កំហុសក្នុងការកត់ត្រាការបណ្តុះបណ្តាល' });
    }
});

// ==========================================
// ចាប់ផ្តើម Server
// ==========================================
app.listen(PORT, async () => {
    await initializeDatabase();
    console.log(`Server connected and running at http://localhost:${PORT}`);
    connectToBiometricMachine();
});
