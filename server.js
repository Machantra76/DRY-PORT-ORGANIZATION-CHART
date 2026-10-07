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

        // ៤. តារាងស្នើសុំច្បាប់សម្រាក
        await client.query(`
            CREATE TABLE IF NOT EXISTS leave_requests (
                id SERIAL PRIMARY KEY,
                employee_code VARCHAR(50) NOT NULL,
                leave_type VARCHAR(50) NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                reason TEXT,
                approval_status VARCHAR(20) DEFAULT 'Pending',
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
// ប្តូរលេខ IP នេះទៅតាម IP ជាក់ស្តែងរបស់ម៉ាស៊ីនស្កេនក្នុងបណ្តាញរបស់អ្នក
const zkInstance = new ZKLib('192.168.1.201', 4370, 10000, 4000);

async function connectToBiometricMachine() {
    try {
        await zkInstance.createSocket();
        console.log('-> បានតភ្ជាប់ជាមួយម៉ាស៊ីនស្កេនវត្តមាន (ZKTeco) ដោយជោគជ័យ!');

        // មុខងារស្តាប់សកម្មភាពស្កេនដោយស្វ័យប្រវត្តិ (Real-time Auto Log)
        zkInstance.getRealTimeLogs(async (data) => {
            console.log('ទទួលបានទិន្នន័យស្កេនថ្មីពីម៉ាស៊ីន:', data);
            
            // ទិន្នន័យពីម៉ាស៊ីនឧ. data.pin (លេខកូដបុគ្គលិក) និង data.time (ម៉ោងស្កេន)
            const employee_code = data.pin;
            const scanTime = new Date(data.time);
            const today = scanTime.toISOString().split('T')[0];

            try {
                // ពិនិត្យមើលថាតើបុគ្គលិកនេះបានស្កេនចូល (Check-in) រួចរាល់ហើយឬยังសម្រាប់ថ្ងៃនេះ
                const checkQuery = `SELECT * FROM attendance_logs WHERE employee_code = $1 AND work_date = $2`;
                const existing = await pool.query(checkQuery, [employee_code, today]);

                if (existing.rows.length === 0) {
                    // ប្រសិនបើទាន់មានការស្កេនទេ គឺកត់ត្រាជា "ម៉ោងចូល (Check-in)"
                    const insertQuery = `
                        INSERT INTO attendance_logs (employee_code, work_date, check_in_time, status) 
                        VALUES ($1, $2, $3, 'Present');
                    `;
                    await pool.query(insertQuery, [employee_code, today, scanTime]);
                    console.log(`[Auto Check-in] បុគ្គលិកកូដ ${employee_code} បានស្កេនចូលម៉ោង ${scanTime}`);
                } else if (!existing.rows[0].check_out_time) {
                    // ប្រសិនបើមាន Check-in ហើយ ប៉ុន្តែទាន់មាន Check-out គឺធ្វើបច្ចុប្បន្នភាពជា "ម៉ោងចេញ (Check-out)"
                    const updateQuery = `
                        UPDATE attendance_logs 
                        SET check_out_time = $1 
                        WHERE employee_code = $2 AND work_date = $3;
                    `;
                    await pool.query(updateQuery, [scanTime, employee_code, today]);
                    console.log(`[Auto Check-out] បុគ្គលិកកូដ ${employee_code} បានស្កេនចេញម៉ោង ${scanTime}`);
                }
            } catch (dbErr) {
                console.error('កំហុសក្នុងការកត់ត្រាទិន្នន័យពីម៉ាស៊ីនស្កេនចូល Database:', dbErr);
            }
        });

    } catch (e) {
        console.log('មិនអាចតភ្ជាប់ទៅកាន់ម៉ាស៊ីនស្កេនបានទេ (សូមពិនិត្យមើល IP Address ឬការតភ្ជាប់ LAN):', e.message);
    }
}

// ==========================================
// API សម្រាប់ Dashboard
// ==========================================
app.get('/api/departments', (req, res) => {
    const data = {
        "dry_port_project": {
            "departments": [
                {
                    "id": "HR_ADMIN",
                    "name": "ធនធានមនុស្ស & រដ្ឋបាល",
                    "roles": [
                        "វត្តមាន ចេញ/ចូល",
                        "ទ្រព្យសម្បត្តិ/ស្តុក និងការគ្រប់គ្រង សារពើភណ្ឌ",
                        "ការជ្រើសរើសបុគ្គលិក & ការបណ្តុះបណ្តាល",
                        "ប្រាក់បៀវត្សរ៍ - ដំណើរការប្រាក់ខែ & អត្ថប្រយោជន៍"
                    ]
                },
                {
                    "id": "ACC_FIN",
                    "name": "គណនេយ្យ & ហិរញ្ញវត្ថុ",
                    "roles": [
                        "គណនេយ្យប្រាក់ចំណូល",
                        "គណនេយ្យទទួល",
                        "របាយការណ៍ហិរញ្ញវត្ថុ",
                        "ការគ្រប់គ្រងថវិកា / ចំណាយ"
                    ]
                },
                {
                    "id": "TRANSPORT",
                    "name": "ផ្នែកដឹកជញ្ជូន",
                    "roles": [
                        "ការចាត់ចែងរថយន្ត & ដឹកជញ្ជូន",
                        "ការគ្រប់គ្រងអ្នកបើកបរ",
                        "ការគ្រប់គ្រងប្រេងឥន្ធនៈ / ការថែទាំ",
                        "ការរៀបចំផែនការដឹកជញ្ជូនទំនិញ"
                    ]
                },
                {
                    "id": "CONTAINER_MGT",
                    "name": "ផ្នែកគ្រប់គ្រងកុងតឺន័រ",
                    "roles": [
                        "ការត្រួតពិនិត្យកុងតឺន័រខូច",
                        "ការគ្រប់គ្រងទីតាំងកុងតឺន័រ",
                        "ការថែទាំ និងជួសជុល",
                        "ការចេញកុងតឺន័រវិញ"
                    ]
                },
                {
                    "id": "TECH_STUFFING",
                    "name": "ផ្នែកត្រួតពិនិត្យទីលានកុងតឺន័រ",
                    "roles": [
                        "ប្រតិបត្តិការនៅទីលាន",
                        "ការចូល / ចេញទំនិញ",
                        "ក្រុមសន្តិសុខ"
                    ]
                },
                {
                    "id": "IT_SYSTEMS",
                    "name": "ផ្នែកព័ត៌មានវិទ្យា & ប្រព័ន្ធ",
                    "roles": [
                        "ប្រព័ន្ធ WMS / OMS",
                        "ប្រព័ន្ធ CCTV & បណ្តាញ",
                        "ការគ្រប់គ្រងទិន្នន័យ"
                    ]
                },
                {
                    "id": "CUSTOMER_SVC",
                    "name": "ផ្នែកសេវាកម្មអតិថិជន",
                    "roles": [
                        "ទំនាក់ទំនងជាមួយអតិថិជន",
                        "ការដោះស្រាយបញ្ហា / ការតវ៉ា",
                        "ជំនួយការផ្នែកស្វែងរកប្រភព"
                    ]
                },
                {
                    "id": "FACILITIES",
                    "name": "ប្រធានផ្នែកសម្ភារឯកសារ",
                    "roles": [
                        "ការគ្រប់គ្រងឯកសារ",
                        "ការគ្រប់គ្រងបញ្ចូលទិន័យប្រព័ន្ធគយ",
                        "អ្នកកាន់ឯកសារ"
                    ]
                }
            ]
        }
    };
    res.json(data);
});

// ==========================================
// API សម្រាប់ Workflow វត្តមាន
// ==========================================
app.get('/api/workflow/hr-attendance', async (req, res) => {
    try {
        const result = await pool.query(
            "SELECT step_no, workflow_stage, action_description FROM department_workflows WHERE department_id = $1 ORDER BY step_no ASC",
            ['HR_ATTENDANCE']
        );
        res.json(result.rows);
    } catch (err) {
        console.error('Database Query Error:', err);
        res.status(500).json({ error: 'មិនអាចទាញយកទិន្នន័យពី Database បានទេ!' });
    }
});

app.post('/api/workflow/hr-attendance', async (req, res) => {
    const { step_no, workflow_stage, action_description } = req.body;
    try {
        const query = `
            INSERT INTO department_workflows (department_id, step_no, workflow_stage, action_description) 
            VALUES ($1, $2, $3, $4) RETURNING *;
        `;
        const values = ['HR_ATTENDANCE', step_no, workflow_stage, action_description];
        const result = await pool.query(query, values);
        
        res.status(201).json({ message: 'បានបញ្ចូលទិន្នន័យដោយជោគជ័យ!', data: result.rows[0] });
    } catch (err) {
        console.error('Insert Error:', err);
        res.status(500).json({ error: 'មិនអាចបញ្ចូលទិន្នន័យទៅក្នុង Database បានទេ!' });
    }
});

// ==========================================
// API សម្រាប់គ្រប់គ្រងបុគ្គលិក និងម៉ោងវត្តមាន (Employees & Attendance)
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
        const query = `
            INSERT INTO employees (employee_code, full_name, department, position) 
            VALUES ($1, $2, $3, $4) RETURNING *;
        `;
        const values = [employee_code, full_name, department, position];
        const result = await pool.query(query, values);
        res.status(201).json({ message: 'បានបន្ថែមបុគ្គលិកដោយជោគជ័យ!', data: result.rows[0] });
    } catch (err) {
        res.status(500).json({ error: 'កំហុសក្នុងការបញ្ចូលបុគ្គលិក (អាចជាន់កូដ)' });
    }
});

app.post('/api/attendance/log', async (req, res) => {
    const { employee_code, action_type } = req.body; 
    const today = new Date().toISOString().split('T')[0];
    const now = new Date();

    try {
        if (action_type === 'IN') {
            const checkQuery = `SELECT * FROM attendance_logs WHERE employee_code = $1 AND work_date = $2`;
            const existing = await pool.query(checkQuery, [employee_code, today]);

            if (existing.rows.length > 0) {
                return res.status(400).json({ error: 'បុគ្គលិកនេះបានស្កេនចូលរួចហើយសម្រាប់ថ្ងៃនេះ!' });
            }

            const insertQuery = `
                INSERT INTO attendance_logs (employee_code, work_date, check_in_time, status) 
                VALUES ($1, $2, $3, 'Present') RETURNING *;
            `;
            const result = await pool.query(insertQuery, [employee_code, today, now]);
            res.status(201).json({ message: 'ស្កេនចូលធ្វើការបានជោគជ័យ!', data: result.rows[0] });

        } else if (action_type === 'OUT') {
            const updateQuery = `
                UPDATE attendance_logs 
                SET check_out_time = $1 
                WHERE employee_code = $2 AND work_date = $3 RETURNING *;
            `;
            const result = await pool.query(updateQuery, [now, employee_code, today]);

            if (result.rows.length === 0) {
                return res.status(404).json({ error: 'មិនទាន់មានទិន្នន័យស្កេនចូលសម្រាប់ថ្ងៃនេះទេ!' });
            }
            res.json({ message: 'ស្កេនចេញពីការងារបានជោគជ័យ!', data: result.rows[0] });
        }
    } catch (err) {
        res.status(500).json({ error: 'មានបញ្ហាក្នុងការកត់ត្រាវត្តមាន!' });
    }
});

app.get('/api/attendance/logs', async (req, res) => {
    try {
        const query = `
            SELECT a.*, e.full_name, e.department 
            FROM attendance_logs a 
            JOIN employees e ON a.employee_code = e.employee_code 
            ORDER BY a.work_date DESC, a.id DESC;
        `;
        const result = await pool.query(query);
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: 'មិនអាចទាញយកទិន្នន័យវត្តមានបានទេ!' });
    }
});

app.listen(PORT, async () => {
    await initializeDatabase();
    console.log(`Server connected and running at http://localhost:${PORT}`);
    
    // ចាប់ផ្តើមភ្ជាប់ទៅកាន់ម៉ាស៊ីនស្កេនវត្តមានដោយស្វ័យប្រវត្តិ
    connectToBiometricMachine();
});