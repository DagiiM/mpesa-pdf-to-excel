- Objectives
This program is to take in a pdf bank statement 

**properties of a given statement**
- May have a password encryption
- Have 100s of pages
- May contain tables well defined and a couple of header columns
- It may have footer and header sections

**System expectations**
- Have chunking capabilities especially not to overwhelm our system.
- We need proper task scheduling, we could use celery.
- We can make use of venv with dedicated start, stop services management.
- Our system is expected to at the end of the day process pdf convert to excel and compute monthly summaries of credits and debits in an excel sheet.
- Minimize the number of packages to be installed.
- Default password is available in password.txt for statement laban.pdf
- No broken windows

- We are tonuse python and strong typing.
- Heavily borrow coding standards from Google.
