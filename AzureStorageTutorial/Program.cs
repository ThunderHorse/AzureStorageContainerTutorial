using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure; //CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; //CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Blob; //Blob storage types

namespace AzureStorageTutorial
{
	public class Program
	{
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
			CloudBlobContainer container = cloudBlobClient.GetContainerReference("myblobstorage");

			container.CreateIfNotExists();

			//Set permissions on cloudBlobContainer
			setContainerPermissions(container);

			//Upload file to block blob
			uploadFile(container);

			//Loop over blob items in container
			loopOverBlobItems(container);
		}

		//Parse the connection string and return a reference to the storage account
		private CloudStorageAccount parseConnectionString()
		{
			CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(
				CloudConfigurationManager.GetSetting("StorageConnectionString"));

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
			var filePath = @"C:\Temp\TestingBlockBlobs.txt";

			//Retrieve reference to blob named "myblob"
			CloudBlockBlob blockBlob = container.GetBlockBlobReference("myblob");

			//Create or overwrite the "myblob" blob with contents from a local file
			using (var fileStream = System.IO.File.OpenRead(filePath))
			{
				blockBlob.UploadFromStream(fileStream);
			}
		}

		//Output length and uri of each blob item
		private void loopOverBlobItems(CloudBlobContainer container)
		{
			foreach (IListBlobItem item in container.ListBlobs(null, false))
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
	}
}
