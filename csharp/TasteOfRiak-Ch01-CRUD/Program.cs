namespace TasteOfRiak_Ch01_CRUD
{
    using System;
    using RiakClient;
    using RiakClient.Models;

    static class Program
    {
        static void Main(string[] args)
        {
            const string bucket = "test";

            try
            {
                IRiakEndPoint endpoint = RiakCluster.FromConfig("riakConfig");
                IRiakClient client = endpoint.CreateClient();

                // Creating Objects In Riak
                Console.WriteLine("Creating Objects In Riak...");

                int val1 = 1;
                var objectId = new RiakObjectId(bucket, "one");
                var riakObject = new RiakObject(objectId, val1);
                var result = client.Put(riakObject);
                CheckResult(result);

                /*
                string val2 = "two";
                myBucket.store("two", val2).execute();

                StringIntMap val3 = new StringIntMap();
                val3.put("myValue", 3);
                myBucket.store("three", val3).execute();


                // Reading Objects From Riak
                Console.WriteLine("Reading Objects From Riak...");

                Integer fetched1 = myBucket.fetch("one", Integer.class.execute();

                IRiakObject fetched2 = myBucket.fetch("two").execute();
                StringIntMap fetched3 = myBucket.fetch("three", StringIntMap.class).execute();

                assert(fetched1 == val1);
                assert(fetched2.getValueAsString().compareTo(val2) == 0);
                assert(fetched3.equals(val3));


                // Updating Objects In Riak
                Console.WriteLine("Updating Objects In Riak");

                fetched3.put("myValue", 42);
                myBucket.store("three", fetched3).execute();


                // Deleting Objects From Riak
                Console.WriteLine("Deleting Objects From Riak...");

                myBucket.delete("one").execute();
                myBucket.delete("two").execute();
                myBucket.delete("three").execute();


                // Working With Complex Objects
                Console.WriteLine("Working With Complex Objects...");

                Book book = new Book();
                book.ISBN = "1111979723";
                book.Title = "Moby Dick";
                book.Author = "Herman Melville";
                book.Body = "Call me Ishmael. Some years ago...";
                book.CopiesOwned = 3;

                Bucket booksBucket = client.fetchBucket("books").execute();
                booksBucket.store(book.ISBN, book).execute();

                IRiakObject riakObject = booksBucket.fetch(book.ISBN).execute();
                Console.WriteLine("Serialized Object:");
                Console.WriteLine("\t" + riakObject.getValueAsString());

                booksBucket.delete(book.ISBN).execute();

                client.shutdown();
                 */
            }
            catch(Exception e)
            {
                Console.Error.WriteLine("Exception: {0}", e.Message);
            }
        }

        private static void CheckResult(RiakResult<RiakObject> result)
        {
            if (!result.IsSuccess)
            {
                Console.Error.WriteLine("Error: {0}", result.ErrorMessage);
                Environment.Exit(1);
            }
        }
    }
}
