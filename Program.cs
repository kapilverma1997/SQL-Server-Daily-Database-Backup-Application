using Dropbox.Api;
using Dropbox.Api.Files;
using Microsoft.Data.SqlClient;
using System.IO.Compression;

namespace SQL_Server_Daily_Database_Backup_Application
{
    internal class Program
    {
        private static string connectionString = "server=sql.infodatixhosting.com;Database=MoldTrax_MRPSolutions_MTLinx2;user id=inxuser; password=password123; integrated security=false; TrustServerCertificate=True";
        private static string backupFolder = @"E:\SQL Server Databases Daily Backup";
        private static string appKey = "5tu5hlabq8ku6bg";
        private static string appSecret = "rl9yda5uh44he0e";
        private static string refreshToken = "dvsLUJDnTtQAAAAAAAAAAV9oOpO_YNsGVh0dxF1B_9B41L6VkpgzHcbX5XMtELXB";
        private static string dropboxBasePath = "/Apps/SQL Server Databases Daily Backup App";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting SQL backup process...");

            try
            {
                Directory.CreateDirectory(backupFolder);

                var databases = GetDatabaseList();
                string dateFolderName = $"SQLBackup_{DateTime.Now:MM_dd_yyyy}";
                string dropboxDateFolderPath = $"{dropboxBasePath}/{dateFolderName}";

                using (var dbx = new DropboxClient(refreshToken, appKey, appSecret, new DropboxClientConfig("SQLServerBackupApp")))
                {
                    await CreateDropboxFolderIfNotExists(dbx, dropboxDateFolderPath);

                    foreach (var dbName in databases)
                    {
                        if (IsSystemDatabase(dbName)) continue;

                        Console.WriteLine($"Backing up database: {dbName}");
                        string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                        string bakFile = Path.Combine(backupFolder, $"{dbName}_{timestamp}.bak");
                        string zipFile = bakFile.Replace(".bak", ".zip");

                        BackupDatabase(dbName, bakFile);

                        using (var zip = ZipFile.Open(zipFile, ZipArchiveMode.Create))
                            zip.CreateEntryFromFile(bakFile, Path.GetFileName(bakFile), CompressionLevel.Optimal);

                        string dropboxFilePath = $"{dropboxDateFolderPath}/{Path.GetFileName(zipFile)}";

                        Console.WriteLine($"Uploading {zipFile} to Dropbox...");
                        await UploadFileToDropboxAsync(dbx, zipFile, dropboxFilePath);
                        Console.WriteLine($"✅ Uploaded {Path.GetFileName(zipFile)} successfully!");

                        File.Delete(bakFile);
                        File.Delete(zipFile);
                    }
                }

                Console.WriteLine("✅ All databases backed up and uploaded successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error: {ex.Message}\n{ex.StackTrace}");
            }
        }

        private static string[] GetDatabaseList()
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();
            using var cmd = new SqlCommand("SELECT name FROM sys.databases WHERE state_desc='ONLINE'", conn);
            using var reader = cmd.ExecuteReader();
            var dbs = new List<string>();
            while (reader.Read())
                dbs.Add(reader.GetString(0));
            return dbs.ToArray();
        }

        private static void BackupDatabase(string databaseName, string backupFilePath)
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();
            string backupSql = $"BACKUP DATABASE [{databaseName}] TO DISK = N'{backupFilePath}' WITH INIT, STATS = 10;";
            using var cmd = new SqlCommand(backupSql, conn);
            cmd.CommandTimeout = 0;
            cmd.ExecuteNonQuery();
        }

        private static bool IsSystemDatabase(string dbName)
        {
            string[] skip = { "master", "model", "msdb", "tempdb", "ContactManagement" };
            return skip.Contains(dbName, StringComparer.OrdinalIgnoreCase);
        }

        private static async Task CreateDropboxFolderIfNotExists(DropboxClient dbx, string folderPath)
        {
            try { await dbx.Files.CreateFolderV2Async(folderPath); }
            catch (Dropbox.Api.ApiException<CreateFolderError>) { /* Folder already exists */ }
        }

        // ✅ Chunked upload implementation
        private static async Task UploadFileToDropboxAsync(DropboxClient dbx, string localFile, string dropboxPath)
        {
            const int chunkSize = 128 * 1024 * 1024; // 128 MB per chunk
            using var fileStream = new FileStream(localFile, FileMode.Open, FileAccess.Read);
            long fileSize = fileStream.Length;

            if (fileSize <= 150 * 1024 * 1024)
            {
                // Simple upload for small files
                await dbx.Files.UploadAsync(
                    dropboxPath,
                    WriteMode.Overwrite.Instance,
                    body: fileStream);
                return;
            }

            // Chunked upload for large files
            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            ulong uploaded = 0;
            UploadSessionStartResult session = null;

            // 1️⃣ Start upload session
            bytesRead = await fileStream.ReadAsync(buffer, 0, chunkSize);
            using (var memStream = new MemoryStream(buffer, 0, bytesRead))
            {
                session = await dbx.Files.UploadSessionStartAsync(body: memStream);
                uploaded += (ulong)bytesRead;
                Console.WriteLine($"Started upload session: {uploaded / (1024 * 1024)} MB uploaded...");
            }

            // 2️⃣ Append chunks
            while ((bytesRead = await fileStream.ReadAsync(buffer, 0, chunkSize)) > 0)
            {
                using var memStream = new MemoryStream(buffer, 0, bytesRead);
                var cursor = new UploadSessionCursor(session.SessionId, uploaded);
                bool isLastChunk = (uploaded + (ulong)bytesRead) >= (ulong)fileSize;

                if (isLastChunk)
                {
                    // 3️⃣ Finish upload session
                    await dbx.Files.UploadSessionFinishAsync(
                        cursor,
                        new CommitInfo(dropboxPath, WriteMode.Overwrite.Instance),
                        body:memStream);
                    Console.WriteLine($"Upload complete: {fileSize / (1024 * 1024)} MB total.");
                }
                else
                {
                    await dbx.Files.UploadSessionAppendV2Async(cursor, body: memStream);
                    uploaded += (ulong)bytesRead;
                    Console.WriteLine($"Uploaded {uploaded / (1024 * 1024)} MB...");
                }
            }
        }
    }
}
