using ApsSettings.Data;
using ApsSettings.Data.Models;
using Bulk_Uploader_Electron.Managers;
using Bulk_Uploader_Electron.Utilities;
using Hangfire;
using Microsoft.EntityFrameworkCore;
using RestSharp;
using Serilog;
using System.Collections.Concurrent;
using System.Net;
using Ac.Net.Authentication;
using Bulk_Uploader_Electron.Helpers;
using Flurl.Http;
using System.Text.RegularExpressions;

namespace mass_upload_via_s3_csharp;

public class FileDTU : IEquatable<FileDTU>
{
    public bool Equals(FileDTU? file)
    {
        if (file is null)
            return false;

        return Name == file.Name
            && ParentFolderUrn == file.ParentFolderUrn
            && ItemId == file.ItemId;
    }

    public override bool Equals(object? obj)
    {
        return obj is FileDTU other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Name, ParentFolderUrn, ItemId);
    }

    public string ProjectId { get; set; } = string.Empty;
    public string ParentFolderUrn { get; set; } = string.Empty;
    public string LocalParentPath { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string ObjectId { get; set; } = string.Empty;
    public string ItemId { get; set; } = string.Empty;
    public string UploadKey { get; set; } = string.Empty;
}

public class HangfireJobs
{
    private const long UploadChunkSize = 20L * 1024 * 1024;
    private const int MaxUploadAttempts = 5;
    private const int MaxUploadUrlBatchSize = 25;

    private static readonly ConcurrentDictionary<int, byte> LocalCrawlStarted = new();

    private readonly DataContext _context;

    public HangfireJobs(DataContext dataContext)
    {
        _context = dataContext ?? throw new ArgumentNullException(nameof(dataContext));
    }

    [Queue("build-autodesk-mirror")]
    public async Task<List<BulkUploadAutodeskMirror>> RetrieveNewMirrorFolders(
        int bulkUploadId,
        int mirrorFolderId)
    {
        var bulkUpload = await _context.BulkUploads
            .FirstOrDefaultAsync(x => x.Id == bulkUploadId);

        var bulkUploadMirror = await _context.BulkUploadAutodeskMirrors
            .FirstOrDefaultAsync(x => x.Id == mirrorFolderId);

        if (bulkUpload == null || bulkUploadMirror == null)
        {
            Log.Warning(
                "Unable to retrieve Autodesk mirror folders. Bulk upload ID: {BulkUploadId}, mirror ID: {MirrorId}",
                bulkUploadId,
                mirrorFolderId);

            return new List<BulkUploadAutodeskMirror>();
        }

        var token = await JointTokenManager.GetToken();

        var (folders, files) = await APSHelpers.GetFolderContents(
            token,
            bulkUpload.ProjectId,
            bulkUploadMirror.FolderUrn);

        var returnFolders = new List<BulkUploadAutodeskMirror>();

        if (files.Count > 0)
        {
            var newBulkUploadMirrorFiles = files
                .Select(file => new BulkUploadAutodeskMirrorFile
                {
                    FolderUrn = bulkUploadMirror.FolderUrn,
                    Name = file.Name.ToLowerInvariant(),
                    ItemId = file.ItemId,
                    BulkUploadId = bulkUpload.Id
                })
                .ToList();

            await _context.BulkUploadAutodeskMirrorFiles
                .AddRangeAsync(newBulkUploadMirrorFiles);
        }

        if (folders.Count > 0)
        {
            var existingFolders = await _context.BulkUploadAutodeskMirrors
                .Where(x => x.BulkUploadId == bulkUploadId)
                .Select(x => x.RelativeFolderPath)
                .ToListAsync();

            var existingFolderPaths = new HashSet<string>(
                existingFolders,
                StringComparer.OrdinalIgnoreCase);

            returnFolders = folders
                .Select(folder => new BulkUploadAutodeskMirror
                {
                    BulkUploadId = bulkUpload.Id,
                    FolderName = folder.Name,
                    FolderUrn = folder.FolderId,
                    FolderUrl = folder.Url,
                    RelativeFolderPath = Path.Combine(
                        bulkUploadMirror.RelativeFolderPath,
                        folder.Name),
                    ContentsRetrieved = false
                })
                .Where(x => !existingFolderPaths.Contains(x.RelativeFolderPath))
                .ToList();

            if (returnFolders.Count > 0)
            {
                await _context.BulkUploadAutodeskMirrors
                    .AddRangeAsync(returnFolders);
            }
        }

        bulkUploadMirror.ContentsRetrieved = true;
        await _context.SaveChangesAsync();

        return returnFolders;
    }

    [Queue("build-autodesk-mirror")]
    public async Task BuildAutodeskMirror(int bulkUploadId, int mirrorId)
    {
        var bulkUploadMirror = await _context.BulkUploadAutodeskMirrors
            .FirstOrDefaultAsync(x => x.Id == mirrorId);

        if (bulkUploadMirror == null)
            return;

        var bulkUpload = await _context.BulkUploads
            .FirstOrDefaultAsync(x => x.Id == bulkUploadMirror.BulkUploadId);

        if (bulkUpload == null)
            return;

        var newBulkUploadMirrors = await RetrieveNewMirrorFolders(
            bulkUploadId,
            mirrorId);

        foreach (var mirror in newBulkUploadMirrors)
        {
            BackgroundJob.Enqueue<HangfireJobs>(
                x => x.BuildAutodeskMirror(bulkUploadId, mirror.Id));
        }

        var remainingJobsCount = await _context.BulkUploadAutodeskMirrors
            .Where(x => x.BulkUploadId == bulkUploadId)
            .CountAsync(x => x.ContentsRetrieved == false);

        if (remainingJobsCount == 0 && LocalCrawlStarted.TryAdd(bulkUpload.Id, 0))
        {
            bulkUpload.AddLogs("Finished creating Autodesk Cloud Folder Mirror");
            await _context.SaveChangesAsync();

            BackgroundJob.Enqueue<HangfireJobs>(
                x => x.ProcessLocalFolder(bulkUpload.Id, bulkUpload.LocalPath));
        }
    }

    [Queue("process-local-folder")]
    public async Task ProcessLocalFolder(int bulkUploadId, string folderPath)
    {
        if (string.IsNullOrWhiteSpace(folderPath) || !Directory.Exists(folderPath))
        {
            Log.Warning(
                "Local folder does not exist. Bulk upload ID: {BulkUploadId}, path: {FolderPath}",
                bulkUploadId,
                folderPath);
            return;
        }

        var bulkUpload = await _context.BulkUploads
            .Include(x => x.AutodeskMirrors)
            .FirstOrDefaultAsync(x => x.Id == bulkUploadId);

        if (bulkUpload == null)
            return;

        var allFolders = Directory.GetDirectories(folderPath).ToList();
        var allFiles = Directory.GetFiles(folderPath).ToList();

        foreach (var folder in allFolders)
        {
            BackgroundJob.Enqueue<HangfireJobs>(
                x => x.ProcessLocalFolder(bulkUploadId, folder));
        }

        var bulkUploadFiles = allFiles
            .Select(filePath =>
            {
                // Store the folder path relative to the selected upload root.
                // A file directly inside the root has an empty relative folder path.
                var relativeFilePath = Path.GetRelativePath(
                    bulkUpload.LocalPath,
                    filePath);

                var relativeFolderPath =
                    Path.GetDirectoryName(relativeFilePath) ?? string.Empty;

                return BulkUploadFile.CreateFile(
                    bulkUpload,
                    relativeFolderPath,
                    filePath);
            })
            .ToList();

        if (bulkUploadFiles.Count > 0)
        {
            await _context.BulkUploadFiles.AddRangeAsync(bulkUploadFiles);
            await _context.SaveChangesAsync();
        }

        foreach (var file in bulkUploadFiles)
        {
            if (bulkUpload.Status == BulkUploadStatus.Preview
                || file.Status == JobFileStatus.DoNotUpload
                || file.Status == JobFileStatus.Failed)
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(file.FolderUrn))
            {
                BackgroundJob.Enqueue<HangfireJobs>(
                    x => x.CreateMissingFolder(file.BulkUploadId, file.Id));
            }
            else
            {
                BackgroundJob.Enqueue<HangfireJobs>(
                    x => x.ProcessFile(file.BulkUploadId, file.Id));
            }
        }
    }

    private List<string> GetAllDirectories(string relativePath)
    {
        var splitPaths = relativePath
            .Split('\\', StringSplitOptions.RemoveEmptyEntries)
            .ToList();

        var directories = new List<string>();
        var currentFolder = string.Empty;

        foreach (var path in splitPaths)
        {
            currentFolder = Path.Combine(currentFolder, path);
            directories.Add(currentFolder);
        }

        return directories;
    }

    [Queue("create-cloud-folders")]
    public async Task CreateMissingFolder(int bulkUploadId, int bulkUploadFileId)
    {
        var bulkUpload = await _context.BulkUploads
            .FirstOrDefaultAsync(x => x.Id == bulkUploadId);

        var bulkUploadFile = await _context.BulkUploadFiles
            .FirstOrDefaultAsync(x => x.Id == bulkUploadFileId);

        if (bulkUpload == null
            || bulkUploadFile == null
            || bulkUpload.Id != bulkUploadFile.BulkUploadId)
        {
            return;
        }

        var paths = GetAllDirectories(bulkUploadFile.TargetRelativePath);
        BulkUploadAutodeskMirror? lastMirror = null;

        foreach (var path in paths)
        {
            var matchingMirror = await _context.BulkUploadAutodeskMirrors
                .FirstOrDefaultAsync(x =>
                    x.BulkUploadId == bulkUploadId
                    && x.RelativeFolderPath == path);

            if (matchingMirror != null)
            {
                lastMirror = matchingMirror;
                continue;
            }

            if (lastMirror == null)
            {
                bulkUploadFile.AddLogs(
                    $"Could not find an existing parent folder for: {path}");
                bulkUploadFile.Status = JobFileStatus.Failed;
                await _context.SaveChangesAsync();
                return;
            }

            var folderName = path.Split('\\').LastOrDefault() ?? string.Empty;

            try
            {
                var folder = await APSHelpers.CreateFolder(
                    bulkUpload.ProjectId,
                    lastMirror.FolderUrn,
                    folderName);

                var newMirrorFolder = new BulkUploadAutodeskMirror
                {
                    BulkUploadId = bulkUploadId,
                    ContentsRetrieved = true,
                    FolderName = folder.Data.Attributes.Name,
                    FolderUrn = folder.Data.Id,
                    FolderUrl = folder.Links.Self.Href,
                    RelativeFolderPath = path
                };

                bulkUploadFile.AddLogs($"Created folder: {path}");

                await _context.BulkUploadAutodeskMirrors.AddAsync(newMirrorFolder);
                await _context.SaveChangesAsync();

                lastMirror = newMirrorFolder;
            }
            catch (FlurlHttpException ex)
            {
                if (ex.StatusCode == 409)
                {
                    // Refresh the mirror after a conflict, then retry this job.
                    await RetrieveNewMirrorFolders(bulkUploadId, lastMirror.Id);
                    throw new InvalidOperationException(
                        $"Folder already exists in Autodesk Construction Cloud: {path}. Mirror refreshed; retry the job.",
                        ex);
                }

                throw;
            }
        }

        if (lastMirror == null)
        {
            bulkUploadFile.AddLogs(
                "Could not find a matching folder in the folder paths.");
            bulkUploadFile.Status = JobFileStatus.Failed;
        }
        else
        {
            bulkUploadFile.AddLogs("Added folder URN");
            bulkUploadFile.FolderUrn = lastMirror.FolderUrn;
            bulkUploadFile.FolderUrl = lastMirror.FolderUrl;
        }

        await _context.SaveChangesAsync();

        if (!string.IsNullOrWhiteSpace(bulkUploadFile.FolderUrn))
        {
            BackgroundJob.Enqueue<HangfireJobs>(
                x => x.ProcessFile(
                    bulkUploadFile.BulkUploadId,
                    bulkUploadFile.Id));
        }
    }

    [Queue("process-file")]
    public async Task CheckForUpdatedFiles(
        int bulkUploadId,
        int bulkUploadFileId)
    {
        var bulkUpload = await _context.BulkUploads
            .FirstOrDefaultAsync(x => x.Id == bulkUploadId);

        var bulkUploadFile = await _context.BulkUploadFiles
            .FirstOrDefaultAsync(x => x.Id == bulkUploadFileId);

        if (bulkUpload == null
            || bulkUploadFile == null
            || bulkUpload.Id != bulkUploadFile.BulkUploadId)
        {
            return;
        }

        var token = await JointTokenManager.GetToken();

        var (folders, files) = await APSHelpers.GetFolderContents(
            token,
            bulkUpload.ProjectId,
            bulkUploadFile.FolderUrn);

        var existingFiles = await _context.BulkUploadAutodeskMirrorFiles
            .Where(x => x.BulkUploadId == bulkUploadId)
            .Where(x => x.FolderUrn == bulkUploadFile.FolderUrn)
            .Select(x => x.Name)
            .ToListAsync();

        var existingFileNames = new HashSet<string>(
            existingFiles,
            StringComparer.OrdinalIgnoreCase);

        var newFiles = files
            .Where(x => !existingFileNames.Contains(x.Name))
            .Select(x => new BulkUploadAutodeskMirrorFile
            {
                BulkUploadId = bulkUploadId,
                FolderUrn = bulkUploadFile.FolderUrn,
                ItemId = x.ItemId,
                Name = x.Name
            })
            .ToList();

        if (newFiles.Count > 0)
        {
            await _context.BulkUploadAutodeskMirrorFiles.AddRangeAsync(newFiles);
            await _context.SaveChangesAsync();
        }

        BackgroundJob.Enqueue<HangfireJobs>(
            x => x.ProcessFile(bulkUploadId, bulkUploadFileId));
    }

    [Queue("process-file")]
    public async Task ProcessFile(int bulkUploadId, int bulkUploadFileId)
    {
        var bulkUpload = await _context.BulkUploads
            .FirstOrDefaultAsync(x => x.Id == bulkUploadId);

        var bulkUploadFile = await _context.BulkUploadFiles
            .FirstOrDefaultAsync(x => x.Id == bulkUploadFileId);

        if (bulkUpload == null
            || bulkUploadFile == null
            || bulkUpload.Id != bulkUploadFile.BulkUploadId)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(bulkUploadFile.FolderUrn))
        {
            await UpdateFileStatus(
                bulkUploadFileId,
                JobFileStatus.Failed,
                "Folder not created yet");
            return;
        }

        var existingFile = await _context.BulkUploadAutodeskMirrorFiles
            .Where(x => x.BulkUploadId == bulkUploadId)
            .Where(x => x.FolderUrn == bulkUploadFile.FolderUrn)
            .FirstOrDefaultAsync(x =>
                x.Name == bulkUploadFile.TargetFileName.ToLowerInvariant());

        if (existingFile != null)
        {
            bulkUploadFile.ItemId = existingFile.ItemId;
            await _context.SaveChangesAsync();
        }

        try
        {
            var storageLocation = await APSHelpers.CreateStorageLocation(
                bulkUpload.ProjectId,
                bulkUploadFile.TargetFileName,
                bulkUpload.FolderId);

            var storageObjectId = storageLocation?.Data?.Id;

            if (string.IsNullOrWhiteSpace(storageObjectId))
            {
                throw new InvalidOperationException(
                    "Autodesk returned an empty storage object ID.");
            }

            var regex = new Regex(
                "^urn:adsk\\.objects:os\\.object:([-_.a-z0-9]{3,128})\\/(.+)$",
                RegexOptions.IgnoreCase);

            var match = regex.Match(storageObjectId);

            if (!match.Success)
            {
                throw new InvalidOperationException(
                    $"The storage object ID has an unexpected format: {storageObjectId}");
            }

            bulkUploadFile.BucketKey = match.Groups[1].Value;
            bulkUploadFile.ObjectId = match.Groups[2].Value;

            await _context.SaveChangesAsync();

            var fileInfo = new FileInfo(bulkUploadFile.SourceAbsolutePath);

            if (!fileInfo.Exists)
            {
                throw new FileNotFoundException(
                    "The source file could not be found.",
                    bulkUploadFile.SourceAbsolutePath);
            }

            var fileSize = fileInfo.Length;
            var numberOfChunks = fileSize == 0
                ? 1
                : (int)Math.Ceiling((double)fileSize / UploadChunkSize);

            long partsUploaded = 0;
            long start = 0;
            var uploadUrls = new List<string>();
            string? uploadKey = null;

            Log.Information(
                "Starting upload of {FilePath}. Size: {FileSize}. Chunks: {ChunkCount}",
                bulkUploadFile.SourceAbsolutePath,
                fileSize,
                numberOfChunks);

            using (var fileStream = new FileStream(
                bulkUploadFile.SourceAbsolutePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read))
            using (var reader = new BinaryReader(fileStream))
            {
                while (partsUploaded < numberOfChunks)
                {
                    var end = Math.Min(
                        (partsUploaded + 1) * UploadChunkSize,
                        fileSize);

                    var numberOfBytes = checked((int)(end - start));
                    var fileBytes = new byte[numberOfBytes];

                    reader.BaseStream.Seek(start, SeekOrigin.Begin);

                    var bytesRead = 0;
                    while (bytesRead < numberOfBytes)
                    {
                        var read = reader.Read(
                            fileBytes,
                            bytesRead,
                            numberOfBytes - bytesRead);

                        if (read == 0)
                        {
                            throw new EndOfStreamException(
                                "Unexpected end of file while reading an upload chunk.");
                        }

                        bytesRead += read;
                    }

                    var chunkUploaded = false;
                    var attempts = 0;

                    while (!chunkUploaded && attempts < MaxUploadAttempts)
                    {
                        attempts++;

                        if (uploadUrls.Count == 0)
                        {
                            var remainingParts = numberOfChunks - partsUploaded;
                            var requestedParts = (int)Math.Min(
                                remainingParts,
                                MaxUploadUrlBatchSize);

                            var firstPartNumber = checked((int)partsUploaded + 1);

                            var uploadParams = await APSHelpers.GetUploadUrls(
                                bulkUploadFile.BucketKey,
                                bulkUploadFile.ObjectId,
                                60,
                                requestedParts,
                                firstPartNumber,
                                uploadKey!);

                            uploadKey = uploadParams.UploadKey;
                            uploadUrls = uploadParams.Urls ?? new List<string>();

                            if (uploadUrls.Count == 0)
                            {
                                throw new InvalidOperationException(
                                    "Autodesk did not return any upload URLs.");
                            }
                        }

                        var currentUrl = uploadUrls[0];
                        uploadUrls.RemoveAt(0);

                        try
                        {
                            var response = await UploadBufferRestSharp(
                                currentUrl,
                                fileBytes);

                            var statusCode = (int)response.StatusCode;

                            if (statusCode >= 200 && statusCode < 300)
                            {
                                chunkUploaded = true;
                                continue;
                            }

                            if (statusCode == 403)
                            {
                                // The signed URL may have expired. Request fresh URLs.
                                uploadUrls.Clear();
                            }
                            else if (statusCode == 503 || statusCode == 429)
                            {
                                await Task.Delay(
                                    TimeSpan.FromSeconds(Math.Min(
                                        Math.Pow(2, attempts),
                                        30)));
                            }
                            else
                            {
                                throw new HttpRequestException(
                                    $"Chunk upload failed with HTTP {statusCode}: {response.Content}");
                            }
                        }
                        catch (Exception ex) when (
                            attempts < MaxUploadAttempts
                            && ex is not OperationCanceledException)
                        {
                            Log.Warning(
                                ex,
                                "Upload chunk attempt {Attempt}/{MaxAttempts} failed for {FilePath}",
                                attempts,
                                MaxUploadAttempts,
                                bulkUploadFile.SourceAbsolutePath);

                            await Task.Delay(
                                TimeSpan.FromSeconds(Math.Min(
                                    Math.Pow(2, attempts),
                                    30)));
                        }
                    }

                    if (!chunkUploaded)
                    {
                        throw new InvalidOperationException(
                            $"Upload chunk {partsUploaded + 1} failed after {MaxUploadAttempts} attempts.");
                    }

                    partsUploaded++;
                    start = end;

                    UploadProgress.Set(
                        bulkUploadId,
                        bulkUploadFileId,
                        end);

                    Log.Information(
                        "Uploaded chunk {CurrentChunk}/{TotalChunks} for {FilePath}",
                        partsUploaded,
                        numberOfChunks,
                        bulkUploadFile.SourceAbsolutePath);
                }
            }

            if (string.IsNullOrWhiteSpace(uploadKey))
            {
                throw new InvalidOperationException(
                    "Autodesk did not return an upload key; the upload cannot be completed.");
            }

            await APSHelpers.CompleteUpload(
                bulkUploadFile.BucketKey,
                bulkUploadFile.ObjectId,
                uploadKey);

            if (string.IsNullOrWhiteSpace(bulkUploadFile.ItemId))
            {
                try
                {
                    var version = await APSHelpers.CreateFirstVersion(
                        bulkUpload.ProjectId,
                        bulkUploadFile.TargetFileName,
                        bulkUploadFile.FolderUrn,
                        bulkUploadFile.BucketKey,
                        bulkUploadFile.ObjectId);

                    bulkUploadFile.VersionId = version.Data.Id;
                    bulkUploadFile.WebUrl = version.Links.Self.Href;
                }
                catch (FlurlHttpException ex) when (ex.StatusCode == 409)
                {
                    BackgroundJob.Enqueue<HangfireJobs>(
                        x => x.CheckForUpdatedFiles(
                            bulkUploadId,
                            bulkUploadFileId));
                    return;
                }
            }
            else
            {
                var version = await APSHelpers.CreateNextVersion(
                    bulkUpload.ProjectId,
                    bulkUploadFile.TargetFileName,
                    bulkUploadFile.ItemId,
                    bulkUploadFile.BucketKey,
                    bulkUploadFile.ObjectId);

                bulkUploadFile.VersionId = version.Data.Id;
                bulkUploadFile.WebUrl = version.Links.Self.Href;
            }

            await _context.SaveChangesAsync();

            await UpdateFileStatus(
                bulkUploadFileId,
                JobFileStatus.Success);
        }
        catch (Exception ex)
        {
            await UpdateFileStatus(
                bulkUploadFileId,
                JobFileStatus.Failed,
                ex.Message);

            Log.Error(
                ex,
                "Problem uploading file: {FilePath}",
                bulkUploadFile.SourceAbsolutePath);

            throw;
        }
        finally
        {
            UploadProgress.Remove(bulkUploadFileId);
        }
    }

    [Queue("kill-finished")]
    public async Task KillOnceFinished(int dbJobId)
    {
        await Task.Delay(2000);

        var api = JobStorage.Current.GetMonitoringApi();

        while (api.ProcessingCount() > 1)
        {
            await Task.Delay(500);
        }

        Log.Information("File Upload Process Completed");

        var job = await _context.BulkUploads.FindAsync(dbJobId);

        if (job == null)
            return;

        job.EndTime = DateTime.Now;
        await _context.SaveChangesAsync();
    }

    /// <summary>
    /// Uploads a single chunk to the supplied signed URL.
    /// </summary>
    /// <param name="url">Signed URL for the upload chunk.</param>
    /// <param name="buffer">Chunk contents.</param>
    public static async Task<RestResponse> UploadBufferRestSharp(
        string url,
        byte[] buffer)
    {
        using var client = new RestClient();
        var request = new RestRequest(url, Method.Put);

        request.AddParameter(
            "application/octet-stream",
            buffer,
            ParameterType.RequestBody);

        // Do not swallow exceptions or recursively retry here.
        // ProcessFile owns the bounded retry policy.
        return await client.ExecuteAsync(request);
    }

    public async Task UpdateFileStatus(
        int dbFileId,
        JobFileStatus status,
        string notes = "")
    {
        try
        {
            var dbFile = await _context.BulkUploadFiles.FindAsync(dbFileId);

            if (dbFile == null)
                return;

            dbFile.Status = status;
            dbFile.LastModified = DateTime.Now;

            if (!string.IsNullOrWhiteSpace(notes))
            {
                dbFile.AddLogs(notes);
            }

            await _context.SaveChangesAsync();
        }
        catch (Exception ex)
        {
            Log.Error(
                ex,
                "Problem updating file status in database for file ID {FileId}",
                dbFileId);
        }
    }
}