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
		private void setContainerPermissions(CloudBlobContainer container)
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

		//Deleting Blob Data
		private void deleteBlobs(CloudBlobContainer container)
		{
			var myBlob = getBlockBlob(container, MY_BLOB_NAME);
		}

		#region Helper Methods
		private CloudBlockBlob getBlockBlob(CloudBlobContainer container, string blobName)
		{
			var blockBlob = container.GetBlockBlobReference(blobName);

			return blockBlob;
		}
		#endregion
	}
}
