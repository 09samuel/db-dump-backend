# db-dump-backend— Database Backup Engine

` db-dump-backend` is the **backend service** for the Database Dump platform.  
It is responsible for **database dump execution, scheduling, compression, checksum validation, and uploading backups to client-managed AWS S3 buckets**.

> This repository contains the **core backup engine**.  
> The frontend / control plane is maintained in a **separate repository**:  
>  **Frontend Repository:** [DatabaseDump](https://github.com/09samuel/DatabaseDump)


---

## Responsibilities

- Execute database dumps for supported databases  
- Schedule and manage automated backup jobs  
- Compress backup files and generate checksums  
- Upload backups to **client-managed AWS S3** using IAM Role assumption  
- Handle restore and download operations  

---

## Supported Databases

- MySQL (`mysqldump`)
- PostgreSQL (`pg_dump`)
- MongoDB (`mongodump`)


---

## Tech Stack

- **Runtime:** Node.js  
- **Framework:** Express.js  
- **Storage:** Client-managed AWS S3  
- **Scheduling:** Cron / background workers  
- **Security:** IAM Role ARN, STS AssumeRole  
- **Compression:** gzip  
- **Integrity:** Checksum validation  

---

## Security Model

- Uses **client-provided IAM Role ARN** to access S3 buckets  
- Clients must create IAM roles with **least-privilege permissions**  
- Access is granted via **temporary credentials using AWS STS**  
- No long-term AWS credentials are stored  
- Database credentials are used only during backup execution  

---

## Backup Workflow

1. Scheduler triggers backup job  
2. Database dump is created using native tools  
3. Dump file is compressed  
4. Checksum is generated for integrity validation  
5. IAM role is assumed via AWS STS  
6. Backup is uploaded to client S3 bucket  
7. Retention policies are applied  

---

## Restore Workflow

1. Backup metadata is selected  
2. Backup is downloaded from S3  
3. Checksum is verified  
4. Backup is decompressed  
5. Database restore is executed  

---

## Performance & Reliability

- Designed to handle **hundreds of backup jobs per day**  
- Achieves **30–50% backup size reduction** through compression  
- Optimized to minimize load on production databases  


---

## Integration

This service is intended to be used alongside the **frontend / control plane** repository, which handles:
- User configuration
- Scheduling rules
- Backup metadata
- Access control


---
## Related Repositories

- **Frontend / Control Plane:**  
  [DatabaseDump](https://github.com/09samuel/DatabaseDump)


