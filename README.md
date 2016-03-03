# Flight Delay Streaming

An Akka Streams application that crunches flight data from a CSV file and emits average delays for each airline carrier.
Akka Streams will ensure that you're never late for an important business trip again!

Recently updated for Akka 2.4.2.

1. Convert each line from a String into a FlightEvent
2. Filter out non-delayed flights from the stream
3. Broadcast the stream across two distinct flows — one to capture raw events, another to capture aggregate flight delay data
4. Emit a substream per airline carrier, accumulating the total number of delayed flights and the minutes of each delay for each airline, then merging the streams together with the totals
5. Print flight delay information to the console

To run this app, you will need to visit http://stat-computing.org/dataexpo/2009/the-data.html and download the CSV file for
a single year. This file should be placed in src/main/resources.

