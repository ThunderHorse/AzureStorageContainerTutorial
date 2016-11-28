using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure; //CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; //CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Blob; //Blob storage types
using System.IO;

namespace AzureStorageTutorial
{
	public class Program
	{
		private const string FILE_PATH = @"C:\Temp\";
		private const string FILE_TO_UPLOAD = "TestingBlockBlobs.txt";
		private const string FILE_TO_DOWNLOAD = "ConfirmingBlockBlobs.txt";

		private const string MY_CONTAINER_NAME = "myblobstorage";
		private const string MY_BLOB_NAME = "myblob";
		private const string APPEND_BLOB_NAME = "append-blob.log";

		private const string STORAGE_ACCT_CONN_STRING_NAME = "StorageConnectionString";

		static void Main(string[] args)
		{
			Program p = new Program();
			p.runProgram();
		}

		private void runProgram()
		{
			//CloudStorageAccount - Reference to cloud storage account application is using
			var cloudStorageAccount = parseConnectionString();

			//CloudBlobClient - Allows you to retrieve containers and blobs stored in Blob storage
			var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

			//CloudBlobContainer - Reference to a container (every blob in Azure storage must reside in a container)
			CloudBlobContainer container = cloudBlobClient.GetContainerReference(MY_CONTAINER_NAME);

			container.CreateIfNotExists();

			//Set permissions on cloudBlobContainer
			setContainerPermissions(container);

			//Upload file to block blob
			uploadFile(container);

			//Loop over blob items in container
			var shouldUseFlatBlobListStyle = false;
			loopOverAndListBlobItems(container, shouldUseFlatBlobListStyle);

			//Loop over blob items in container and list as flat
			shouldUseFlatBlobListStyle = true;
			loopOverAndListBlobItems(container, shouldUseFlatBlobListStyle);

			//Download blobs
			downloadBlobs(container);

			//List blobs in pages asynchronously
			//NOTE: Something is off with this method, await is not awaiting
			//await listBlobsSegmentedInFlatListing(container);

			//Write to append blob
			writeToAppendBlob(container);

			//Delete blobs
			deleteBlobs(container);


		}

		//Parse the connection string and return a reference to the storage account
		private CloudStorageAccount parseConnectionString()
		{
			CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(
				CloudConfigurationManager.GetSetting(STORAGE_ACCT_CONN_STRING_NAME));

			return cloudStorageAccount;
		}

		//Change blob container permission to public access
		private static void setContainerPermissions(CloudBlobContainer container)
		{
			//NOTE: Anyone on the Internet can see blobs in a public container,
			//but you can modify or delete them only if you have the appropriate
			//account access key or a shared access signature.
			container.SetPermissions(new BlobContainerPermissions
			{
				PublicAccess = BlobContainerPublicAccessType.Blob
			});
		}

		//Upload file to block blob
		private void uploadFile(CloudBlobContainer container)
		{
			//Retrieve reference to blob named "myblob"
			var blockBlob = getBlockBlob(container, MY_BLOB_NAME);

			//Create or overwrite the "myblob" blob with contents from a local file
			using (var fileStream = System.IO.File.OpenRead(FILE_PATH + FILE_TO_UPLOAD))
			{
				blockBlob.UploadFromStream(fileStream);
			}
		}

		//Output length and uri of each blob item
		private void loopOverAndListBlobItems(CloudBlobContainer container, bool shouldUseFlatBlobListing)
		{
			foreach (IListBlobItem item in container.ListBlobs(null, shouldUseFlatBlobListing))
			{
				if (item.GetType() == typeof(CloudBlockBlob))
				{
					CloudBlockBlob blob = (CloudBlockBlob)item;

					Console.WriteLine("Block blob of length {0}: {1}", blob.Properties.Length, blob.Uri);
				}
				else if (item.GetType() == typeof(CloudPageBlob))
				{
					CloudPageBlob pageBlob = (CloudPageBlob)item;

					Console.WriteLine("Page blob of length {0}: {1}", pageBlob.Properties.Length, pageBlob.Uri);
				}
				else if (item.GetType() == typeof(CloudBlobDirectory))
				{
					CloudBlobDirectory directoryBlob = (CloudBlobDirectory)item;

					Console.WriteLine("Directory: {0}", directoryBlob.Uri);
				}
			}
		}

		//Downloading Blob Data
		private void downloadBlobs(CloudBlobContainer container)
		{
			var downloadFilePath = FILE_PATH + FILE_TO_DOWNLOAD;

			var blockBlob = getBlockBlob(container, MY_BLOB_NAME);

			//To in memory stream
			string text;
			using (var memoryStream = new MemoryStream())
			{
				blockBlob.DownloadToStream(memoryStream);
				text = System.Text.Encoding.UTF8.GetString(memoryStream.ToArray());

				Console.WriteLine("Downloaded to stream text: {0}", text);
			}

			//To physical filepath
			using (var fileStream = System.IO.File.OpenWrite(downloadFilePath))
			{
				blockBlob.DownloadToStream(fileStream);

				Console.WriteLine("Path to downloaded file: {0}", downloadFilePath);
			}
		}

		async private static Task<BlobResultSegment> listBlobsSegmentedInFlatListing(CloudBlobContainer container)
		{
			Console.WriteLine("List blobs in pages: ");

			int i = 0;
			BlobContinuationToken continuationToken = null;
			BlobResultSegment resultSegment = null;

			//Call ListBlobsSegmentedAsync and enumerate the result segment returned, while the continuation token is non-null.
			//When the continuation token is null, the last page has been returned and execution can exit loop
			do
			{
				//This overload allows control of the page size. You can return all remaining results by passing null for the maxResults parameter,
				//or by calling a different overload.
				resultSegment = await container.ListBlobsSegmentedAsync("", true, BlobListingDetails.All, 10, continuationToken, null, null);

				if (resultSegment.Results.Count<IListBlobItem>() > 0)
				{
					Console.WriteLine("Page {0}: ", i++);
				}

				foreach (var blobItem in resultSegment.Results)
				{
					Console.WriteLine("\t{0}", blobItem.StorageUri.PrimaryUri);
				}
				Console.WriteLine();

				//Get the continuation token
				continuationToken = resultSegment.ContinuationToken;
			}
			while (continuationToken != null);

			return resultSegment;
		}

		private void writeToAppendBlob(CloudBlobContainer container)
		{
			//Get a reference to the append blob
			var appendBlob = container.GetAppendBlobReference(APPEND_BLOB_NAME);

			//Create the append blob. Note that if the blob already exists, the CreateOrReplace() method will overwrite it.
			//You can check whether the blob exist to avoid overwriting it by using CloudAppendBlob.Exist()
			appendBlob.CreateOrReplace();

			int numBlocks = 10;

			//Generate an array of random bytes
			Random rnd = new Random();
			byte[] bytes = new byte[numBlocks];
			rnd.NextBytes(bytes);

			//Simulate a logging operation by writing text data and byte data to the end of the append blob.
			for (int i = 0; i < numBlocks; i++)
			{
				appendBlob.AppendText(String.Format("Timestamp: {0:u} \tLog Entry: {1}{2}", DateTime.UtcNow, bytes[i], Environment.NewLine));
			}

			//Read the append blob to the console window
			Console.WriteLine(appendBlob.DownloadText());
		}

		//Deleting Blob Data
		private void deleteBlobs(CloudBlobContainer container)
		{
			var myBlob = getBlockBlob(container, MY_BLOB_NAME);

			myBlob.Delete();

			var appendBlob = getBlockBlob(container, APPEND_BLOB_NAME);

			appendBlob.Delete();
		}

		#region Helper Methods
		private static CloudBlockBlob getBlockBlob(CloudBlobContainer container, string blobName)
		{
			var blockBlob = container.GetBlockBlobReference(blobName);

			return blockBlob;
		}
		#endregion
	}
}
