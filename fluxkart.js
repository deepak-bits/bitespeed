const express = require('express');
const mysql = require('mysql2');
const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.json());

// Create connection
// const con = mysql.createConnection({
//     host: 'localhost',
//     user: 'root',
//     password: '',
//     database: 'fluxKartDb'
// });

const con = mysql.createConnection({
    host: 'sql12.freesqldatabase.com',
    user: 'sql12628584',
    password: 'NgGAAUVzLk',
    database: 'sql12628584'
});

// Connect
con.connect((err) => {
    if(err) throw err;
    console.log('MySQL connected...');
    let sql = `
        CREATE TABLE IF NOT EXISTS Contact (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            phoneNumber VARCHAR(255),
            email VARCHAR(255),
            linkedId INT,
            linkPrecedence ENUM("secondary", "primary"),
            createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updatedAt TIMESTAMP NOT NULL,
            deletedAt TIMESTAMP
        )
    `;

    con.query(sql, (err, result) => {
        if (err) throw err;
        console.log("Table created");
    });
});

app.post('/identify', (req, res) => {
    let { email, phoneNumber } = req.body;

    phoneNumber = phoneNumber.toString();

    if (!email && !phoneNumber) {
        return res.status(400).json({ error: 'Email or phoneNumber is required' });
    }

    let primaryContactId;
    let primaryContactIdEmail;
    let primaryContactIdPhone;
    const emails = [];
    const phoneNumbers = [];
    const secondaryContactIds = [];

    let emailObj;
    let phoneNumberObj;

    // Function to retrieve primary contact based on email or phoneNumber
    function retrievePrimaryContact() {
        return new Promise((resolve, reject) => {
        if (email) {
            con.query(
            'SELECT * FROM Contact WHERE email = ? AND linkPrecedence = "primary"',
            [email],
            (err, rows) => {
                if (err) {
                    console.error('Error retrieving primary contact:', err);
                    reject(err);
                } else {
                    if (rows.length > 0) {
                        emailObj = rows[0];
                        primaryContactIdEmail = rows[0].id;
                        !emails.includes(rows[0].email) && emails.push(rows[0].email);
                        !phoneNumbers.includes(rows[0].phoneNumber) && phoneNumbers.push(rows[0].phoneNumber);
                    }
                }
            }
            );
        }
    
        if (phoneNumber) {
            con.query(
            'SELECT * FROM Contact WHERE phoneNumber = ? AND linkPrecedence = "primary"',
            [phoneNumber],
            (err, rows) => {
                if (err) {
                    console.error('Error retrieving primary contact:', err);
                    reject(err);
                } else {
                    if (rows.length > 0) {
                        phoneNumberObj = rows[0];
                        primaryContactIdPhone = rows[0].id;
                        !emails.includes(rows[0].email) && emails.push(rows[0].email);
                        !phoneNumbers.includes(rows[0].phoneNumber) && phoneNumbers.push(rows[0].phoneNumber);
                    }
                    resolve();
                }
            }
            );
        }
        });
    }

    // Find secondary contacts linked to the primary contact
    function executeSecondaryContactsQuery() {
        return new Promise((resolve, reject) => {
            if(primaryContactIdEmail && primaryContactIdPhone) {
                if(primaryContactIdEmail === primaryContactIdPhone) {
                    primaryContactId = primaryContactIdEmail;
                    resolve();
                } else {
                    const time1 = emailObj.createdAt;
                    const time2 = phoneNumberObj.createdAt;

                    if(time1.getTime() < time2.getTime()) {
                        // email object was added earlier
                        con.query(
                            'UPDATE Contact SET linkPrecedence = "secondary", linkedId = ?, updatedAt = CURRENT_TIMESTAMP WHERE id = ?',
                            [emailObj.id, phoneNumberObj.id],
                            (err, rows) => {
                                if (err) {
                                    console.error('Error updating linkPrecedence:', err);
                                    reject(err);
                                } else {
                                    primaryContactId = emailObj.id;
                                    secondaryContactIds.push(phoneNumberObj.id);
                                    console.log('LinkPrecedence updated successfully');

                                    resolve();

                                }
                            }
                        );                          
                    } else {
                        // phoneNumber object was added earlier
                        con.query(
                            'UPDATE Contact SET linkPrecedence = "secondary", linkedId = ?, updatedAt = CURRENT_TIMESTAMP WHERE id = ?',
                            [phoneNumberObj.id, email.id],
                            (err, rows) => {
                                if (err) {
                                    console.error('Error updating linkPrecedence:', err);
                                    reject(err);
                                } else {
                                    primaryContactId = phoneNumberObj.id;
                                    secondaryContactIds.push(emailObj.id);
                                    console.log('LinkPrecedence updated successfully');

                                    resolve();

                                }
                            }
                        );      
                    }

                }
            } else {
                if(primaryContactIdEmail && !primaryContactIdPhone) primaryContactId = primaryContactIdEmail;
                if(primaryContactIdPhone && !primaryContactIdEmail) primaryContactId = primaryContactIdPhone;

                if (primaryContactId) {
                    con.query(
                        'SELECT * FROM Contact WHERE linkedId = ?',
                        [primaryContactId],
                        (err, rows) => {
                            if (err) {
                                console.error('Error retrieving secondary contacts:', err);
                                reject(err);
                            }
    
                            rows.forEach((row) => {
                                !emails.includes(row.email) && emails.push(row.email);
                                !phoneNumbers.includes(row.phoneNumber) && phoneNumbers.push(row.phoneNumber);
                                secondaryContactIds.push(row.id);
                            });
                        }
                    );
    
                    con.query(
                        'SELECT * FROM Contact WHERE linkedId = ? AND linkPrecedence = "secondary"',
                        [primaryContactId],
                        (err, rows) => {
                            if(err) {
                                reject(err);
                            } else {
                                if(rows.length > 0) {
                                    resolve();
                                } else {
                                    con.query(
                                        'INSERT INTO Contact (phoneNumber, email, linkedId, linkPrecedence, updatedAt) VALUES (?, ?, ?, "secondary", CURRENT_TIMESTAMP)',
                                        [phoneNumber, email, primaryContactId],
                                        (err, result) => {
                                            if (err) {
                                                console.error('Error creating new primary contact:', err);
                                                reject(err);
                                            }
                                            
                                            !emails.includes(email) && emails.push(email);
                                            !phoneNumbers.includes(phoneNumber) && phoneNumbers.push(phoneNumber);
                                            secondaryContactIds.push(result.insertId);
                                            resolve();
                                        }
                                    );
                                }
                            }
                        }
                    )
                    
                } else {
                    // Create a new primary contact if no existing primary contact found
                    con.query(
                        'SELECT * FROM Contact WHERE email = ? AND phoneNumber = ? AND linkPrecedence = "secondary"',
                        [email, phoneNumber],
                        (err, results) => {
                            if(err) {
                                reject(err);
                            } else {
                                if(results.length > 0) {
                                    primaryContactId = results[0].linkedId;
                                    con.query(
                                        'SELECT * FROM Contact WHERE id = ? AND linkPrecedence = "primary"',
                                        [primaryContactId],
                                        (err, rows) => {
                                            if(err) {
                                                reject(err);
                                            } else {
                                                
                                                if(rows.length > 0) {
                                                    !emails.includes(rows[0].email) && emails.push(rows[0].email);
                                                    // console.log(emails);
                                                    !phoneNumbers.includes(rows[0].phoneNumber) && phoneNumbers.push(rows[0].phoneNumber);
                                                }

                                                !emails.includes(results[0].email) && emails.push(results[0].email);
                                                !phoneNumbers.includes(results[0].phoneNumber) && phoneNumbers.push(results[0].phoneNumber);
                                                secondaryContactIds.push(results[0].id);
                                                resolve();
                                            }
                                        }
                                    )
                                    
                                } else {
                                    con.query(
                                        'INSERT INTO Contact (phoneNumber, email, linkedId, linkPrecedence, updatedAt) VALUES (?, ?, NULL, "primary", CURRENT_TIMESTAMP)',
                                        [phoneNumber, email],
                                        (err, result) => {
                                            if (err) {
                                                console.error('Error creating new primary contact:', err);
                                                reject();
                                            }
                    
                                            primaryContactId = result.insertId;
                                            !emails.includes(email) && emails.push(email);
                                            !phoneNumbers.includes(phoneNumber) && phoneNumbers.push(phoneNumber);
                                            resolve();
                                        }
                                    );
                                }
                            }
                        }
                    )

                    
                }
            }     
            

            
        })
    }

    async function processContacts() {
        try {
            await retrievePrimaryContact();
            await executeSecondaryContactsQuery();
        
            const contact = {
                primaryContactId,
                emails,
                phoneNumbers,
                secondaryContactIds,
            };
        
            return res.json({ contact });
        } catch (err) {
            console.error('Error processing contacts:', err);
            return res.status(500).json({ error: 'Internal server error' });
        }
    }

    processContacts();     
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});


