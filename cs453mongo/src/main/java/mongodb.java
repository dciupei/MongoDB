import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.BasicDBObjectBuilder;

import java.util.Calendar;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;
import java.text.ParseException;



public class mongodb {

    public static void query_A(MongoClient mongoClient){
        DB db = mongoClient.getDB("freeway_loop");
        DBCollection coll = db.getCollection("loop_data");
        BasicDBObject query = new BasicDBObject("speed", new BasicDBObject("$gt", 100));
        DBCursor cursor = coll.find(query);

        int i = 0;
        while (cursor.hasNext()){
            i++;
            cursor.next();
        }
        System.out.print("Query_A: ");
        System.out.print("Total speeds > 100 = " + i + "\n");
    }

    public static void query_B(MongoClient mongoClient) {
        DB db = mongoClient.getDB("freeway_detect");
        DBCollection coll = db.getCollection("detectors");
        BasicDBObject query = new BasicDBObject("locationtext", new BasicDBObject("$eq", "Foster NB"));
        DBCursor cursor = coll.find(query);
        int Total_Volume = 0;

        for (DBObject doc : cursor) {
            int detectorid = (Integer) doc.get("detectorid");

            DB db2 = mongoClient.getDB("Freeway_loop_T");
            DBCollection coll2 = db2.getCollection("Truncated_Loop");
            BasicDBObject query2 = new BasicDBObject("detectorid", new BasicDBObject("$eq", detectorid));
            Pattern regex = Pattern.compile("9/21/11");
            query2.put("starttime", regex);
            DBCursor cursor2 = coll2.find(query2);

            while (cursor2.hasNext()) {
                BasicDBObject temp = (BasicDBObject) cursor2.next();
                String volume = temp.getString("volume");
                if(volume.length() == 0) {
                    continue;
                }else {
                    //System.out.print(speed2 + "\n");
                    int curr_volume = Integer.parseInt(volume);
                    Total_Volume = Total_Volume + curr_volume;

                }
            }

        }
        System.out.print("Query_B: ");
        System.out.print("Total volume for 9/21/11 on Foster NB is " + Total_Volume + "\n");
    }

    public static void query_C(MongoClient mongoClient) throws ParseException{
        DB db = mongoClient.getDB("freeway_stations");
        DBCollection coll = db.getCollection("stations");
        BasicDBObject query = new BasicDBObject("locationtext", new BasicDBObject("$eq", "Foster NB"));
        DBCursor cursor = coll.find(query);

        Double station_Length = 0.0;            //length of Foster NB
        int stationID = 0;
        for (DBObject station_L : cursor) {
            station_Length = (Double) station_L.get("length");
            stationID = (Integer) station_L.get("stationid");
        }
        //System.out.print(stationID);
        //System.out.print(station_Length);
        DB db2 = mongoClient.getDB("freeway_detect");
        DBCollection coll2 = db2.getCollection("detectors");
        BasicDBObject query2 = new BasicDBObject("stationid", new BasicDBObject("$eq", stationID));
        DBCursor cursor2 = coll2.find(query2);

        int num_OfSpeeds = 0;
        int Speed_total = 0;

        //String endtime = "";
        String endtimeT;


        String endtime2 = "2011-09-23 00:00:00-07";
        SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss-SS");
        java.util.Date utilDate1 = df1.parse(endtime2);
        java.sql.Date EndDate = new java.sql.Date(utilDate1.getTime());
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(EndDate);
        endtimeT = df1.format(cal1.getTime());
       // System.out.print(endtimeT + "\n");

        //String starttime = "2011-09-22 00:00:00-07";
        double traveltime = 0;

        //int detectorid = 0;
        List<Integer> detectorid = new ArrayList<Integer>();

        for (DBObject temp : cursor2) {
            int detectorID = (Integer) temp.get("detectorid");
            detectorid.add(detectorID);

        }
        //System.out.print(detectorid + "\n");

        DB db3 = mongoClient.getDB("Freeway_loop_partD");
        DBCollection coll3 = db3.getCollection("Truncated_QueryD");
        //while(!endtime.equals(endtimeT)) {
            for (int i = 0; i < detectorid.size(); i++) {
                String starttime = "2011-09-22 00:00:00-07";

                String endtime = "2011-09-22 00:00:00-07";

                //System.out.print("endtime = " + endtime + "\n");
                //System.out.print("starttime = " + starttime + "\n");
                while(!endtime.equals(endtimeT)) {


                    java.sql.Date sqld;

                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss-SS");
                    java.util.Date utilDate = df.parse(starttime);
                    java.sql.Date sqlStartDate = new java.sql.Date(utilDate.getTime());
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(sqlStartDate);
                    cal.add(Calendar.MINUTE, 5);
                    endtime = df.format(cal.getTime());
                    //System.out.print("endtime = " + endtime + "\n");
                    //System.out.print("starttime = " + starttime + "\n");

                    //System.out.print(endtimeT + "\n");

                    //System.out.print(detectorid + "\n");

                    BasicDBObject query3 = new BasicDBObject("detectorid", new BasicDBObject("$eq", detectorid.get(i)))
                            .append("starttime", BasicDBObjectBuilder.start("$gte", starttime).add("$lte", endtime).get());

                    DBCursor cursor3 = coll3.find(query3);


                    while (cursor3.hasNext()) {
                        BasicDBObject temp2 = (BasicDBObject) cursor3.next();
                        String speed = temp2.getString("speed");
                        if (speed.length() == 0) {
                            //System.out.print("cool" + "\n");

                            continue;
                        } else {
                            //System.out.print(speed + "\n");
                            num_OfSpeeds++;
                            int curr_Speed = Integer.parseInt(speed);
                            Speed_total = Speed_total + curr_Speed;

                        }

                    }
                    

                    java.util.Date d = df.parse(endtime);
                    d.getTime();
                    sqld = new java.sql.Date(d.getTime());
                    Calendar cald = Calendar.getInstance();
                    cald.setTime(sqld);
                    endtime = df.format(cald.getTime());
                    starttime = endtime;



                }

                double average_speed = Speed_total / num_OfSpeeds;
                traveltime = (station_Length / average_speed) * 3600;

        }

        System.out.print("Query_C: ");
        System.out.print("Total travel time in 5 minute intervals in seconds for Foster NB is " + traveltime +" seconds" + "\n");
    }



    public static void query_D(MongoClient mongoClient) {
        DB db = mongoClient.getDB("freeway_stations");
        DBCollection coll = db.getCollection("stations");
        BasicDBObject query = new BasicDBObject("locationtext", new BasicDBObject("$eq", "Foster NB"));
        DBCursor cursor = coll.find(query);

        Double station_Length = 0.0;            //length of Foster NB
        int stationID = 0;
        for (DBObject station_L : cursor) {
            station_Length = (Double) station_L.get("length");
            stationID = (Integer) station_L.get("stationid");
        }
        //System.out.print(stationID);
        //System.out.print(station_Length);
        DB db2 = mongoClient.getDB("freeway_detect");
        DBCollection coll2 = db2.getCollection("detectors");
        BasicDBObject query2 = new BasicDBObject("stationid", new BasicDBObject("$eq", stationID));
        DBCursor cursor2 = coll2.find(query2);

        int num_OfSpeeds = 0;
        int Speed_total = 0;
        List<Integer> detectorid = new ArrayList<Integer>();

        for (DBObject temp : cursor2) {
            int detectorID = (Integer) temp.get("detectorid");
            detectorid.add(detectorID);
        }
            //System.out.print(detectorid + "\n");
            for (int i = 0; i < detectorid.size(); i++) {
//            DB db3 = mongoClient.getDB("Freeway_loopData");
//            DBCollection coll3 = db3.getCollection("loop_data");
                DB db3 = mongoClient.getDB("Freeway_loop_partD");
                DBCollection coll3 = db3.getCollection("Truncated_QueryD");
                BasicDBObject query3 = new BasicDBObject("detectorid", new BasicDBObject("$eq", detectorid.get(i)))
                        .append("starttime", BasicDBObjectBuilder.start("$gte", "2011-09-22 07:00:00-07").add("$lte", "2011-09-22 09:00:00-07").get());
                BasicDBObject query4 = new BasicDBObject("detectorid", new BasicDBObject("$eq", detectorid.get(i)))
                        .append("starttime", BasicDBObjectBuilder.start("$gte", "2011-09-22 16:00:00-07").add("$lte", "2011-09-22 18:00:00-07").get());
                DBCursor cursor3 = coll3.find(query3);
                DBCursor cursor4 = coll3.find(query4);

                while (cursor3.hasNext()) {
                    BasicDBObject temp2 = (BasicDBObject) cursor3.next();
                    String speed = temp2.getString("speed");
                    if (speed.length() == 0) {
                        continue;
                    } else {
                        //System.out.print(speed + "\n");
                        num_OfSpeeds++;
                        int curr_Speed = Integer.parseInt(speed);
                        Speed_total = Speed_total + curr_Speed;

                    }
                }
                while (cursor4.hasNext()) {
                    BasicDBObject temp3 = (BasicDBObject) cursor4.next();
                    String speed2 = temp3.getString("speed");
                    if (speed2.length() == 0) {
                        continue;
                    } else {
                        //System.out.print(speed2 + "\n");
                        num_OfSpeeds++;
                        int curr_Speed = Integer.parseInt(speed2);
                        Speed_total = Speed_total + curr_Speed;

                    }

                }
            }


        //System.out.print(num_OfSpeeds + "\n");
        //System.out.print(Speed_total + "\n");
            double average_speed = Speed_total / num_OfSpeeds;
            //System.out.print(average_speed + "\n");
            double traveltime = (station_Length / average_speed) * 3600;

            System.out.print("Query_D: ");
            System.out.print("Total travel time in seconds for Foster NB is " + traveltime + " seconds" + "\n");

    }

    public static void query_E(MongoClient mongoClient) {
        DB db = mongoClient.getDB("Highways_");
        DBCollection coll = db.getCollection("highway");
        BasicDBObject query = new BasicDBObject("highwayname", new BasicDBObject("$eq", "I-205"))
                .append("direction", new BasicDBObject("$eq", "NORTH"));
        DBCursor cursor = coll.find(query);

        int highwayid = 0;
        for (DBObject highway : cursor) {
            highwayid = (Integer) highway.get("highwayid");
        }


        DB db1 = mongoClient.getDB("freeway_stations");
        DBCollection coll1 = db1.getCollection("stations");
        BasicDBObject query1 = new BasicDBObject("highwayid", new BasicDBObject("$eq", highwayid));
        DBCursor cursor1 = coll1.find(query1);

        List<Double> station_Length_Total = new ArrayList<Double>();
        List<Integer> stationid = new ArrayList<Integer>();

        for (DBObject station_L : cursor1) {
            Double station_Length = (Double) station_L.get("length");
            station_Length_Total.add(station_Length);
            //System.out.print(station_Length + "\n");

            int stationID = (Integer) station_L.get("stationid");
            stationid.add(stationID);
        }
        //System.out.println(stationid);

        //System.out.print(station_Length_Total);
        List<Integer> detectorid = new ArrayList<Integer>();
        List<Integer> speeds = new ArrayList<Integer>();
        List<Integer> speed_TotalCount = new ArrayList<Integer>();

        DB db2 = mongoClient.getDB("freeway_detect");
        DBCollection coll2 = db2.getCollection("detectors");


        for(int i = 0; i < stationid.size(); i++) {
            int num_OfSpeeds = 0;
            int Speed_total = 0;
            detectorid.clear();

            BasicDBObject query2 = new BasicDBObject("stationid", new BasicDBObject("$eq", stationid.get(i)));
            DBCursor cursor2 = coll2.find(query2);

            for (DBObject temp : cursor2) {
                int detectorID = (Integer) temp.get("detectorid");
                detectorid.add(detectorID);             // get the speed and length for each station over here and add the sum to arraylist
                //System.out.print(detectorid + "\n"+ "\n");
            }

        for (int j = 0; j < detectorid.size(); j++) {
            DB db3 = mongoClient.getDB("Freeway_loop_partD");
            DBCollection coll3 = db3.getCollection("Truncated_QueryD");
            BasicDBObject query3 = new BasicDBObject("detectorid", new BasicDBObject("$eq", detectorid.get(j)))
                    .append("starttime", BasicDBObjectBuilder.start("$gte", "2011-09-22 07:00:00-07").add("$lte", "2011-09-22 09:00:00-07").get());
            BasicDBObject query4 = new BasicDBObject("detectorid", new BasicDBObject("$eq", detectorid.get(j)))
                    .append("starttime", BasicDBObjectBuilder.start("$gte", "2011-09-22 16:00:00-07").add("$lte", "2011-09-22 18:00:00-07").get());
//            BasicDBObject query3 = new BasicDBObject("detectorid", new BasicDBObject("$eq", detectorid.get(j)))
//                    .append("starttime", BasicDBObjectBuilder.start("$gte", "9/22/11 7:00").add("$lte", "9/22/11 9:00").get());
//            BasicDBObject query4 = new BasicDBObject("detectorid", new BasicDBObject("$eq", detectorid.get(j)))
//                    .append("starttime", BasicDBObjectBuilder.start("$gte", "9/22/11 16:00").add("$lte", "9/22/11 18:00").get());
            DBCursor cursor3 = coll3.find(query3);
            DBCursor cursor4 = coll3.find(query4);

            while (cursor3.hasNext()) {
                BasicDBObject temp2 = (BasicDBObject) cursor3.next();
                String speed = temp2.getString("speed");
                if (speed.length() == 0) {
                    continue;
                } else {
                    //System.out.print(speed + "\n");
                    num_OfSpeeds++;
                    int curr_Speed = Integer.parseInt(speed);
                    Speed_total = Speed_total + curr_Speed;


                }
            }
            while (cursor4.hasNext()) {
                BasicDBObject temp3 = (BasicDBObject) cursor4.next();
                String speed2 = temp3.getString("speed");
                if (speed2.length() == 0) {
                    continue;
                } else {
                    //System.out.print(speed2 + "\n");
                    num_OfSpeeds++;
                    int curr_Speed = Integer.parseInt(speed2);
                    Speed_total = Speed_total + curr_Speed;


                }
            }

        }
        speed_TotalCount.add(num_OfSpeeds);
        speeds.add(Speed_total);

        }
//        System.out.println(speeds);
//        System.out.println(speed_TotalCount);
//        System.out.println(station_Length_Total);

        double average_Speed = 0;
        double travel_Time = 0;
        for (int i = 0; i < speeds.size(); i++){
            if (speed_TotalCount.get(i) == 0){
                continue;
            }
            average_Speed = speeds.get(i) / speed_TotalCount.get(i);
            travel_Time = station_Length_Total.get(i) / average_Speed;
        }
        //System.out.println(travel_Time);

        //System.out.println(average_Speed);
        travel_Time = travel_Time * 60;
        //System.out.println(travel_Time);

        System.out.print("Query_E: ");
        System.out.print("Total travel time in minutes for I-205 NB is " + travel_Time + " minutes"+"\n");
    }

    public static void query_F(MongoClient mongoClient) {
        DB db = mongoClient.getDB("Highways_");
        DBCollection coll = db.getCollection("highway");
        BasicDBObject query = new BasicDBObject("highwayname", new BasicDBObject("$eq", "I-205"))
                .append("direction", new BasicDBObject("$eq", "NORTH"));
        DBCursor cursor = coll.find(query);

        int highwayid = 0;
        for (DBObject highway : cursor) {
            highwayid = (Integer) highway.get("highwayid");
        }
        System.out.print("Query_F:" + "\n");



        DB db1 = mongoClient.getDB("freeway_stations");
        DBCollection coll1 = db1.getCollection("stations");
        BasicDBObject query1 = new BasicDBObject("highwayid", new BasicDBObject("$eq", highwayid));
        Pattern regex = Pattern.compile("Johnson");
        query1.put("locationtext", regex);
        DBCursor cursor1 = coll1.find(query1);

        int Downstream = 0;
        for (DBObject direction : cursor1) {
            Downstream = (Integer) direction.get("downstream");
            System.out.print("Start: " + "\n\n" + "Johnson CR using downstream " + Downstream + " to" + "\n");

            //System.out.print(Downstream);
        }

        String Destination = "";

        while (Downstream != -1) {

            DB db2 = mongoClient.getDB("freeway_stations");
            DBCollection coll2 = db2.getCollection("stations");
            BasicDBObject query2 = new BasicDBObject("stationid", new BasicDBObject("$eq", Downstream));
            DBCursor cursor2 = coll2.find(query2);


            for (DBObject direction2 : cursor2) {
                Downstream = (Integer) direction2.get("downstream");
                //System.out.print(Downstream + "\n");
                Destination = (String) direction2.get("locationtext");
                if(Destination.equals("Columbia to I-205 NB")){
                    break;
                }
                System.out.print("\n"+Destination + " which will then use " + Downstream + " to"+"\n");

            }
            if(Destination.equals("Columbia to I-205 NB")){
                    break;
            }

        }
        System.out.print("\n"+ "Columbia BLVD, destination has been reached!" + "\n");


    }

        public static void main(String[] args) {
            MongoClient mongoClient = new MongoClient("localhost", 27017);
            long startTime = System.nanoTime();
            query_A(mongoClient);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.

            System.out.println("That took " + duration + " milliseconds" + "\n");

            startTime = System.nanoTime();
            query_B(mongoClient);
            endTime = System.nanoTime();
            duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.

            System.out.println("That took " + duration + " milliseconds"+ "\n");


            try {
                startTime = System.nanoTime();

                query_C(mongoClient);

                endTime = System.nanoTime();
                duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
                System.out.println("That took " + duration + " milliseconds"+ "\n");


            }catch (Exception e){
                e.printStackTrace();

            }

            startTime = System.nanoTime();
            query_D(mongoClient);
            endTime = System.nanoTime();
            duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.

            System.out.println("That took " + duration + " milliseconds"+ "\n");

            startTime = System.nanoTime();
            query_E(mongoClient);
            endTime = System.nanoTime();
            duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.

            System.out.println("That took " + duration + " milliseconds" + "\n");

            startTime = System.nanoTime();
            query_F(mongoClient);
            endTime = System.nanoTime();
            duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
            System.out.println("That took " + duration + " milliseconds" + "\n");


        }

}