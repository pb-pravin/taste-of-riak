namespace TasteOfRiak_Ch01_CRUD
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
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
                var objectId1 = new RiakObjectId(bucket, "one");
                var riakObject1 = new RiakObject(objectId1, val1);
                var result = client.Put(riakObject1);
                CheckResult(result);

                string val2 = "two";
                var objectId2 = new RiakObjectId(bucket, "two");
                var riakObject2 = new RiakObject(objectId2, val2);
                result = client.Put(riakObject2);
                CheckResult(result);

                var val3 = new Dictionary<string, int>
                {
                    { "myValue1", 3 },
                    { "myValue2", 4 }
                };
                var objectId3 = new RiakObjectId(bucket, "three");
                var riakObject3 = new RiakObject(objectId3, val3);
                result = client.Put(riakObject3);
                CheckResult(result);

                Console.WriteLine("Reading Objects From Riak...");

                var fetchResult1 = client.Get(objectId1);
                CheckResult(fetchResult1);
                RiakObject fetchObject1 = fetchResult1.Value;
                int fetchVal1 = fetchObject1.GetObject<int>();
                Debug.Assert(val1 == fetchVal1, "Assert Failed", "val1 {0} != fetchVal1 {1}", val1, fetchVal1);

                var fetchResult2 = client.Get(objectId2);
                CheckResult(fetchResult2);
                RiakObject fetchObject2 = fetchResult2.Value;
                string fetchVal2 = fetchObject2.GetObject<string>();
                Debug.Assert(val2 == fetchVal2, "Assert Failed", "val2 {0} != fetchVal2 {1}", val2, fetchVal2);

                var fetchResult3 = client.Get(objectId3);
                CheckResult(fetchResult3);
                RiakObject fetchObject3 = fetchResult3.Value;
                var fetchVal3 = fetchObject3.GetObject<Dictionary<string, int>>();
                Debug.Assert(val3.Count == fetchVal3.Count && !val3.Except(fetchVal3).Any(), "Assert Failed", "val3 {0} != fetchVal3 {1}", val3, fetchVal3);

                /*
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
            catch (Exception e)
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
