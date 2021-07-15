using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace Company.Function
{
    public static class BlobTrigger1
    {
        [FunctionName("BlobTrigger1")]
        public static async Task Run([BlobTrigger("%source-container-name%/{name}", Connection = "source-storage-connection-string")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            string SourceAccountConnString = System.Environment.GetEnvironmentVariable("source-storage-connection-string");
            string SourceContainerName= System.Environment.GetEnvironmentVariable("source-container-name");
            string TargetAccountConnString = System.Environment.GetEnvironmentVariable("target-storage-connection-string");
            string TargetContainerName = System.Environment.GetEnvironmentVariable("target-container-name");

            BlobContainerClient SourceContainerClient=new BlobContainerClient(SourceAccountConnString,SourceContainerName);
            BlobContainerClient TargetContainerClient=new BlobContainerClient(TargetAccountConnString,TargetContainerName);

            await CopyBlobAsync(SourceContainerClient,TargetContainerClient, name,log);
            

        }





        private static async Task CopyBlobAsync(BlobContainerClient sourceContainer,BlobContainerClient targetContainer,string blobName,ILogger log)
{
    try
    {
        // Get the name of the first blob in the container to use as the source.
        //string blobName = sourceContainer.GetBlobs().FirstOrDefault().Name;

        // Create a BlobClient representing the source blob to copy.
        BlobClient sourceBlob = sourceContainer.GetBlobClient(blobName);

        // Get a BlobClient representing the destination blob.
        BlobClient destBlob = targetContainer.GetBlobClient( blobName);


        // Ensure that the source blob exists.
        if (await sourceBlob.ExistsAsync())
        {
            // Lease the source blob for the copy operation 
            // to prevent another client from modifying it.
            BlobLeaseClient lease = sourceBlob.GetBlobLeaseClient();

            // Specifying -1 for the lease interval creates an infinite lease.
            await lease.AcquireAsync(TimeSpan.FromSeconds(-1));

            // Get the source blob's properties and display the lease state.
            BlobProperties sourceProperties = await sourceBlob.GetPropertiesAsync();
            log.LogInformation($"Lease state: {sourceProperties.LeaseState}");

            //Generate  sas uri for the source blob inorder to copy blob from different storage account
            var sourceSas = sourceBlob.GenerateSasUri(Azure.Storage.Sas.BlobSasPermissions.Read, DateTime.UtcNow.AddMinutes(-10).AddDays(7));

           
            // Start the copy operation.
            await destBlob.StartCopyFromUriAsync(sourceSas);

            // Get the destination blob's properties and display the copy status.
            BlobProperties destProperties = await destBlob.GetPropertiesAsync();

            log.LogInformation($"{blobName} Copy status: {destProperties.CopyStatus},Copy progress: {destProperties.CopyProgress},Completion time: {destProperties.CopyCompletedOn},Total bytes: {destProperties.ContentLength}");
           

            // Update the source blob's properties.
            sourceProperties = await sourceBlob.GetPropertiesAsync();

            if (sourceProperties.LeaseState == LeaseState.Leased)
            {
                // Break the lease on the source blob.
                await lease.BreakAsync();

                // Update the source blob's properties to check the lease state.
                sourceProperties = await sourceBlob.GetPropertiesAsync();
                log.LogInformation($"{blobName} Lease state: {sourceProperties.LeaseState}");
            }
        }
    }
    catch (RequestFailedException ex)
    {
        log.LogError(ex.Message);
        
    }
}
    }
}
