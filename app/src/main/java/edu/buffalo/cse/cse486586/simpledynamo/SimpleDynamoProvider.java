package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    ArrayList<String> avdList = new ArrayList<String>();
    final String[] ports = {"11124", "11112", "11108", "11116", "11120"};
    final String[] nodeName = {"5562", "5556", "5554", "5558", "5560"};
    ArrayList<String> nodeList = null;
    ArrayList<String> hashList = new ArrayList<String>();
    String curNode;
    ConcurrentHashMap<String, String> localDB = new ConcurrentHashMap<String, String>();
    ConcurrentHashMap<String, String> replicaDB = new ConcurrentHashMap<String, String>();
//    ConcurrentHashMap<String, String> replica2 = new ConcurrentHashMap<String, String>();
    ArrayList<String> container = new ArrayList<String>();
    String[] reqColumns = {DBHandler.COL_NAME_KEY, DBHandler.COL_NAME_VALUE};

    DBHandler mDBhandler = null;
    final String STORE_COR = "store_coordinator";
    final String STORE_REPLICA_1 = "store_replica one";
    final String STORE_REPLICA_2 = "store_replica two";
    final String QUERY_CUSTOM = "query_custom";
    final String QUERY_HM_RESULT = "query_hm_result";
    final String QUERY_ONE = "query_one";
    final String QUERY_FOUND_KEY = "query_found_key";
    final String QUERY_FOUND_SUCC = "query_found_succ";
    public static final String TYPE_COR = "cor";
    public static final String TYPE_REP1 = "rep1";
    public static final String TYPE_REP2 = "rep2";
    public static final String DELETE_IN_COR = "delete in cor";
    public static final String DELETE_IN_REPLICA_1 = "delete in replica 1";
    public static final String DELETE_IN_REPLICA_2 = "delete in replica 2";
    public static final String INITIAL = "INITIAL";
    public static final String RECOVER_COR = "RECOVER_COR";
    public static final String RECOVERY_REP = "RECOVER_REP";
    public static final String QUERY_ONE_SUCC = "QUERY_ONE_SUCC";
    public static final String QUERY_STAR_RESULT = "QUERY_STAR_RESULT";
    public static final String QUERY_STAR_RESULT_NEW = "QUERY_STAR_RESULT_NEW";
    String curKey ="";
    ContentResolver cr = null;
    Cursor cursor;
    MatrixCursor mCursor = null;
    String curPort = null;
    Object lockInsert = new Object();
    Object lockQuery = new Object();
    volatile boolean stopFlag = false;


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        //initialization
        mDBhandler = new DBHandler(getContext());
        cr =  (this.getContext()).getContentResolver();

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        curNode = String.valueOf((Integer.parseInt(portStr)));  //5554
        curPort = String.valueOf((Integer.parseInt(portStr) * 2));   //11108
        try {   //server task
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e("Exception", "server Socket Exception");
        }
        //adding elements with hash value..
        nodeList = new ArrayList(Arrays.asList(nodeName));
        for(String n : nodeList) {
            try {
                hashList.add(genHash(n));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        for(int i = 0; i < 5; i++) {
            try {
                Log.i(TAG, "Node and Hash values are " + i + "::" + nodeList.get(i) + "::" + genHash(nodeList.get(i)));

                String portToSend = nodeList.get(i);
                if(portToSend.compareTo(curNode) != 0) {    //send only for other ports
                    String msgToSend = INITIAL + ";;" + curNode + "==" + (Integer.parseInt(portToSend) * 2);
                    Log.i(TAG, "broadcasting in on create with msg:" + msgToSend);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
//                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portToSend) * 2);
//                    OutputStream outToAnother = socket.getOutputStream();
//    //                Log.i("Port Name", (Integer.parseInt(portToSend) * 2));
//                    outToAnother.write(msgToSend.getBytes());
//                    socket.close();
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (Exception e) {
                Log.e(TAG, "exception in oncreate " + e);
            }

        }
        for(int i = 0; i < 5; i++) {
            Log.i(TAG, "avd and Hash values are " + nodeList.get(i) + "::" + hashList.get(i));
        }

        Log.i(TAG, "check getNodeid for 5562:" + getNodeId("5562"));
        Log.i(TAG, "check getNodeid for 5556:" + getNodeId("5556"));
        Log.i(TAG, "check getNodeid for 5554:" + getNodeId("5554"));
        Log.i(TAG, "check getNodeid for 5558:" + getNodeId("5558"));
        Log.i(TAG, "check getNodeid for 5560:" + getNodeId("5560"));


        return false;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.i(TAG, "values are sent to insert " + values.toString());
        Log.i(TAG, "values are sent to insert " + values.toString());
        synchronized (lockInsert) {
            try {

                String key = (String) (values.get("key"));
                String value = (String) (values.get("value"));
                Log.i(TAG, "key is " + key + "; value is " + value);
                String insertNode = getNodeId(key);
                int index = nodeList.indexOf(insertNode);
                String replica1 = nodeList.get((index + 1) % 5);
                String replica2 = nodeList.get((index + 2) % 5);
                Log.i(TAG, "index insert Node and replica nodes are " + index + ":" + insertNode + ":" + replica1 + ":" + replica2);
                SQLiteDatabase db;
                Log.i(TAG, "insert node is " + insertNode + ";; curNode is " + curNode);
                if (insertNode.compareTo(curNode) == 0) { //insert locally
                    Log.i(TAG, "insert Node in the if loop");

                    try {
                        db = mDBhandler.getWritableDatabase();
                        localDB.put(key, value);
                        //setting the new content values with col TYPE
                        ContentValues cv = new ContentValues();
                        cv.put(DBHandler.COL_NAME_KEY, key);
                        cv.put(DBHandler.COL_NAME_VALUE, value);
//                        cv.put(DBHandler.COL_TYPE, TYPE_COR);   //setting as coordinator
                        Log.i(TAG, "content values which are inserted : " + cv);
                        //hack for insert as well as updating the table with single entry
//                    synchronized(lockInsert) {
                        long rowId = db.insertWithOnConflict(DBHandler.TABLE_NAME, null, cv, android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE);
                        if (rowId == -1) {   //value already exists
                            Log.i("Conflict", "Error inserting values in DB");
                        } else {
                            Log.i(TAG, "success fully inserted " + values.toString());
                        }
//                    }

                    } catch (Exception e) {
                        Log.v(TAG, "Exception while inserting :" + e);
                    }

                    //send to replica_1
//                synchronized(lockInsert) {
                    Integer portToSend1 = Integer.parseInt(replica1) * 2;
                    String msgToSend1 = STORE_REPLICA_1 + ";;" + key + ";;" + value + "==" + portToSend1;
                    Log.i(TAG, "insert in if part :" + msgToSend1);
//                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portToSend1);
//                    OutputStream outToAnother1 = socket1.getOutputStream();
//                    Log.i("Port Name", String.valueOf(portToSend1));
//                    outToAnother1.write(msgToSend1.getBytes());
//                    socket1.close();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend1);

                    //send to replica2
                    Integer portToSend2 = Integer.parseInt(replica2) * 2;   //to get the port numbers
                    String msgToSend2 = STORE_REPLICA_2 + ";;" + key + ";;" + value + "==" + portToSend2;
                    Log.i(TAG, "insert in if part :" + msgToSend2);
//                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portToSend2);
//                    OutputStream outToAnother2 = socket2.getOutputStream();
//                    Log.i("Port Name", String.valueOf(portToSend2));
//                    outToAnother2.write(msgToSend2.getBytes());
//                    socket2.close();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend2);
//                }

                    return uri;
                } else {
                    Log.i(TAG, "insert Node in the else loop");
//                synchronized(lockInsert) {
                    Integer portToSend = Integer.parseInt(insertNode) * 2;
                    String msgToSend = STORE_COR + ";;" + key + ";;" + value + "==" + portToSend;
                    Log.i(TAG, "insert  :" + msgToSend + "::" + portToSend);
//                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portToSend);
//                    OutputStream outToAnother = socket.getOutputStream();
//                    Log.i("Port Name", String.valueOf(portToSend));
//                    outToAnother.write(msgToSend.getBytes());
//                    socket.close();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);

                    //send to replica_1
                    Integer portToSend1 = Integer.parseInt(replica1) * 2;
                    String msgToSend1 = STORE_REPLICA_1 + ";;" + key + ";;" + value + "==" + portToSend1;
                    Log.i(TAG, "insert in if part :" + msgToSend1);
//                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portToSend1);
//                    OutputStream outToAnother1 = socket1.getOutputStream();
//                    Log.i("Port Name", String.valueOf(portToSend1));
//                    outToAnother1.write(msgToSend1.getBytes());
//                    socket1.close();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend1);

                    //send to replica2
                    Integer portToSend2 = Integer.parseInt(replica2) * 2;   //to get the port numbers
                    String msgToSend2 = STORE_REPLICA_2 + ";;" + key + ";;" + value + "==" + portToSend2;
                    Log.i(TAG, "insert in if part :" + msgToSend2);
//                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portToSend2);
//                    OutputStream outToAnother2 = socket2.getOutputStream();
//                    Log.i("Port Name", String.valueOf(portToSend2));
//                    outToAnother2.write(msgToSend2.getBytes());
//                    socket2.close();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend2);
//                }

                }
            } catch (Exception e) {
                Log.e(TAG, "Exception in on insert :" + e);
            }
        }
        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                                     String[] selectionArgs, String sortOrder) {
        synchronized (lockQuery) {
            String[] reqColumns = {DBHandler.COL_NAME_KEY, DBHandler.COL_NAME_VALUE};
            SQLiteDatabase db;
            mCursor = new MatrixCursor(new String[]{DBHandler.COL_NAME_KEY, DBHandler.COL_NAME_VALUE});

            try {
                db = mDBhandler.getReadableDatabase();
                String key = selection;
                if (selection.equals("\"@\"")) {
                    Log.i(TAG, "Query: selecting all the data from the current AVD");
                    cursor = db.query(DBHandler.TABLE_NAME, reqColumns, null, null, null, null, null);
                    Log.i(TAG, "length of cursor :" + cursor.getColumnCount());
                    return cursor;
                } else if (selection.equals("\"*\"")) {
                    Log.i(TAG, "Query: selecting data from ALL the AVD");
                    cursor = db.query(DBHandler.TABLE_NAME, reqColumns, null, null, null, null, null);
                    //add it to the matrix cursor
                    cursor.moveToFirst();
                    for (int move = 0; move < cursor.getCount(); move++) {
                        String keyTemp = cursor.getString(cursor.getColumnIndex(DBHandler.COL_NAME_KEY));
                        String valueTemp = cursor.getString(cursor.getColumnIndex(DBHandler.COL_NAME_VALUE));
                        mCursor.addRow(new String[]{keyTemp, valueTemp});
                        cursor.moveToNext();
                    }
                    for (String port : ports) {
//                        String portToSend = String.valueOf((Integer.parseInt(port) * 2));
                        String msgToSend = QUERY_CUSTOM + ";;" + (Integer.parseInt(curNode) * 2);
                        Log.i(TAG, "query key-valuefor each loop  :" + msgToSend);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                        OutputStream outToAnother = socket.getOutputStream();
                        Log.i(TAG, "port in the query for loop:" + port);
                        outToAnother.write(msgToSend.getBytes());
                        socket.close();
                    }
                    Thread.sleep(2000);
                    Log.i(TAG, "container is NOT empty;; size:" + container.size());
                    for (String ele : container) {
                        mCursor.addRow(ele.split(";;"));    //adding the elements in the cursor
                    }
                    Log.i(TAG, "matrix cursor count is :" + mCursor.getCount());
                    return mCursor;
                } else {
                    Log.i(TAG, "Query: else part getting one key");
                    Log.i(TAG, "Query: Selection:" + selection);
                    if (localDB.containsKey(key) || replicaDB.containsKey(key)) {      //contains the key
                        Log.i(TAG, "Query contains in the localDB " + key);
                        cursor = db.query(DBHandler.TABLE_NAME, null, DBHandler.COL_NAME_KEY + "=" + "'" + selection + "'", null, null, null, null);
                        cursor.moveToFirst();
                        Log.i(TAG, "cursor values for the key : " + key + "::" + DatabaseUtils.dumpCursorToString(cursor));
                        return cursor;
                    } else {    //search in the next node
                        Log.i(TAG, "Query does not contain in the localDB " + selection);
                        String portToSend = getNodeId(key);


                        curKey = selection;
                        stopFlag = false;
                        String colnames[] = {"key", "value"};
                        mCursor = new MatrixCursor(colnames);

                        String msgToSend = QUERY_ONE + ";;" + key + ";;" + curPort + "==" + (Integer.parseInt(portToSend) * 2);
                        Log.i(TAG, "query key-value  :" + msgToSend);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);

                        // send to successor
                        int index = nodeList.indexOf(portToSend);
                        String succOfToSend = nodeList.get((index + 1) % 5);
                        msgToSend = QUERY_ONE_SUCC + ";;" + key + ";;" + curPort + "==" + (Integer.parseInt(succOfToSend) * 2);
                        Log.i(TAG, "query key-value for the succ  :" + msgToSend);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);

                        container.clear();  //empty the arraylist
                        while (!stopFlag) {
                            //wait till the key is obtained
                        }
                        Log.i(TAG, "stop Flag has been reset");
//                        for (String ele : container) {
//                            mCursor.addRow(ele.split(";;"));    //adding the elements in the cursor
//                        }
                        curKey = "";
                        mCursor.moveToFirst();
                        System.out.println("FINAL" + DatabaseUtils.dumpCursorToString(mCursor));
                        return mCursor;

                    }
                }

        }catch(InterruptedException e){
            e.printStackTrace();
        }catch(UnknownHostException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }
        return null;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        SQLiteDatabase db;
        db = mDBhandler.getWritableDatabase();
        String key = selection;
        try {
            Log.i(TAG, "key is " + key);
            if (selection.equals("\"@\"") || selection.equals("\"*\"") ) {
                Log.i(TAG, "Delete: selecting all the data from the current AVD");
                db.delete(DBHandler.TABLE_NAME, null, null);
            }
            else if(localDB.contains(key)){ //
                try {
                    db = mDBhandler.getWritableDatabase();
                    localDB.remove(key);
                    db.delete(DBHandler.TABLE_NAME, DBHandler.COL_NAME_KEY + "=" + "'" + key + "'", null);
                    //deleting the replica entries too
                    int index = nodeList.indexOf(curNode);
                    String replica1 = nodeList.get((index + 1) % 5);
                    String replica2 = nodeList.get((index + 2) % 5);
                    String msgToSend = DELETE_IN_REPLICA_1 + ";;" + key + "==" + (Integer.parseInt(replica1) * 2);
                    Log.i(TAG, "DELETING in replica 1 " + msgToSend);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);

                    String msgToSend2 = DELETE_IN_REPLICA_2 + ";;" + key + "==" + (Integer.parseInt(replica2) * 2);
                    Log.i(TAG, "DELETING in replica 2 " + msgToSend2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend2);

                } catch (Exception e) {
                    Log.e(TAG, "Exception while deleting :" + e);
                }
            } else {    //delete in the corresponding avd
                try {
                    String deleteNode = getNodeId(key);
                    int index = nodeList.indexOf(deleteNode);
                    String replica1 = nodeList.get((index + 1) % 5);
                    String replica2 = nodeList.get((index + 2) % 5);
                    String msgToSend = DELETE_IN_COR + ";;" + key + "==" + (Integer.parseInt(deleteNode) * 2);
                    Log.i(TAG, "DELETING in cor " + msgToSend);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);

                    String msgToSend1 = DELETE_IN_REPLICA_1 + ";;" + key + "==" + (Integer.parseInt(replica1) * 2);
                    Log.i(TAG, "DELETING in replica 1 " + msgToSend1);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend1);

                    String msgToSend2 = DELETE_IN_REPLICA_2 + ";;" + key + "==" + (Integer.parseInt(replica2) * 2);
                    Log.i(TAG, "DELETING in replica 2 " + msgToSend2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend2);

                } catch (Exception e) {
                    Log.e(TAG, "Exception while deleting in else part " + e);
                }
            }

        } catch (Exception e) {
            Log.e(TAG, "Exception in delete " + e);
        }
        return 0;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    //helper classes
    class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        String TAG = ServerTask.class.getSimpleName();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            Socket server;
            try {
                // While loop to repeatedly check for socket connectivity
                while (true) {
                    server = serverSocket.accept();
                    InputStream inFromAnother = server.getInputStream();
                    StringBuilder sb = new StringBuilder();
                    int v;
                    while ((v = inFromAnother.read()) != -1) {
                        char ch = (char) v;
                        sb.append(ch);
                    }
                    String msgFromStream = sb.toString();
                    Log.i("output", sb.toString());
                    String msgRead[] = msgFromStream.trim().split(";;");
                    String signal = msgRead[0];
                    String key, value;
                    ContentValues cv = null;
                    Uri newUri;
                    SQLiteDatabase db;
                    Cursor tempCursor;
                    switch (signal) {
                        case INITIAL:   //INITIAL + ";;" + curNode + "==" + (Integer.parseInt(portToSend) * 2);
                            db = mDBhandler.getReadableDatabase();
                            String[] reqColumns = {DBHandler.COL_NAME_KEY, DBHandler.COL_NAME_VALUE};
                            tempCursor = db.query(DBHandler.TABLE_NAME, reqColumns, null, null, null, null, null);
                            Log.i(TAG, "tempcursor count " + tempCursor.getCount());
                            if(tempCursor.getCount() > 0) {    //test if the content provider is empty or not
                                String toSendPort = msgRead[1]; //555*
                                int index = nodeList.indexOf(toSendPort);
                                String succ1 = nodeList.get((index + 1) % 5);
                                String succ2 = nodeList.get((index + 2) % 5);
                                String pred1 = nodeList.get((index + 5 - 1) % 5);
                                String pred2 = nodeList.get((index + 5 - 2) % 5);
                                Log.i(TAG, "toSendport : " + toSendPort + " pred1 :" + pred1  + " pred2 :" + pred2 + " succ1 : " + succ1 + " succ2 : " + succ2);
                                if(curNode.compareTo(succ1) == 0 || curNode.compareTo(succ2) == 0) {    //send the replica to get the cordinator
                                    String corValues = replicaDB.toString();
                                    String msgToSend = RECOVER_COR + ";;" + corValues + "==" + (Integer.parseInt(toSendPort) * 2);
                                    Log.i(TAG, "recovery for cor is " + msgToSend);
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
                                } else if(curNode.compareTo(pred1) == 0 || curNode.compareTo(pred2) == 0) {     //send the coordinator to get the replica
                                    //send the replica
                                    String repValues = localDB.toString();
                                    String msgToSend = RECOVERY_REP + ";;" + repValues + "==" + (Integer.parseInt(toSendPort) * 2);
                                    Log.i(TAG, "recovery for rep is " + msgToSend);
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
                                }
                             }
                            break;
                        case RECOVER_COR:   //RECOVER_COR + ";;" + corValues + "==" + (Integer.parseInt(toSendPort) * 2);
                            Log.i(TAG, "update the values of local db cor");
                            new UpdateLocalDBTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgRead[1]);
                            break;
                        case RECOVERY_REP: //RECOVERY_REP + ";;" + repValues + "==" + (Integer.parseInt(toSendPort) * 2);
                            Log.i(TAG, "update the values in replica DB");
                            new UpdateReplica1DBTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgRead[1]);
                            break;
                        case  STORE_COR:    //STORE_COR + ";;" + key + ";;" + value + "==" + portToSend;
                            synchronized(lockInsert) {
                                key = msgRead[1];
                                value = msgRead[2];
                                localDB.put(key, value);
                                db = mDBhandler.getWritableDatabase();
                                cv = new ContentValues();
                                cv.put(DBHandler.COL_NAME_KEY, key);
                                cv.put(DBHandler.COL_NAME_VALUE, value);
//                                cv.put(DBHandler.COL_TYPE, TYPE_COR);       //setting as coordinator
                                Log.i(TAG, "Content values in coordinator is " + cv);
                                long rowId = db.insertWithOnConflict(DBHandler.TABLE_NAME, null, cv, android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE);
                                Log.v("insert", "insert since one node" + cv.toString());
                                if (rowId == -1) {   //value already exists
                                    Log.i("Conflict", "Error inserting values in DB");
                                } else {
                                    Log.i(TAG, "success fully inserted in replica 1" + cv.toString());
                                }
                            }
//                            newUri = cr.insert(
//                                    buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider"),
//                                    cv
//                            );
                            break;
                        case STORE_REPLICA_1:     // STORE_REPLICA_2 + ";;" + key + ";;" + value
                            synchronized(lockInsert) {
                                String key1 = msgRead[1];
                                String value1 = msgRead[2];
                                replicaDB.put(key1, value1);
                                Log.i(TAG, "key values to be in replica 1 is " + key1 + "::" + value1);
                                db = mDBhandler.getWritableDatabase();
                                cv = new ContentValues();
                                cv.put(DBHandler.COL_NAME_KEY, key1);
                                cv.put(DBHandler.COL_NAME_VALUE, value1);
//                                cv.put(DBHandler.COL_TYPE, TYPE_REP1);       //setting as Replica1
                                Log.i(TAG, "Content values in replica 1 is " + cv);
                                long rowId1 = db.insertWithOnConflict(DBHandler.TABLE_NAME, null, cv, android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE);
                                Log.v("insert", "insert since one node" + cv.toString());
                                if (rowId1 == -1) {   //value already exists
                                    Log.i("Conflict", "Error inserting values in DB");
                                } else {
                                    Log.i(TAG, "success fully inserted in replica 1" + cv.toString());
                                }
                            }
//
//                            newUri = cr.insert(
//                                    buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider"),
//                                    cv
//                            );
                            //wrting with new Col type
//                            cv = new ContentValues();
//                            cv.put(DBHandler.COL_NAME_KEY, key1);
//                            cv.put(DBHandler.COL_NAME_VALUE, value1);
//                            cv.put(DBHandler.COL_TYPE, TYPE_REP1);   //setting as replica 1
//                            Log.i(TAG, "Content values in replica 1 is " + cv);
//                            newUri = cr.insert(
//                                    buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider"),
//                                    cv
//                            );
                            break;
                        case STORE_REPLICA_2:     // STORE_REPLICA_2 + ";;" + key + ";;" + value
                            synchronized(lockInsert) {
                                String key2 = msgRead[1];
                                String value2 = msgRead[2];
                                replicaDB.put(key2, value2);
                                Log.i(TAG, "key values to be in replica 2 is " + key2 + "::" + value2);
                                db = mDBhandler.getWritableDatabase();
                                cv = new ContentValues();
                                cv.put(DBHandler.COL_NAME_KEY, key2);
                                cv.put(DBHandler.COL_NAME_VALUE, value2);
//                                cv.put(DBHandler.COL_TYPE, TYPE_REP1);       //setting as coordinator
                                long rowId2 = db.insertWithOnConflict(DBHandler.TABLE_NAME, null, cv, android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE);
                                Log.v("insert", "insert since one node" + cv.toString());
                                if (rowId2 == -1) {   //value already exists
                                    Log.i("Conflict", "Error inserting values in DB");
                                } else {
                                    Log.i(TAG, "success fully inserted in replica 2" + cv.toString());
                                }
                            }
//                            Log.i(TAG, "Content values in coordinator is " + cv);
//                            newUri = cr.insert(
//                                    buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider"),
//                                    cv
//                            );
                            //writing with new col values
//                            cv = new ContentValues();
//                            cv.put(DBHandler.COL_NAME_KEY, key2);
//                            cv.put(DBHandler.COL_NAME_VALUE, value2);
//                            cv.put(DBHandler.COL_TYPE, TYPE_REP2);   //setting as replica 2
//                            Log.i(TAG, "Content values in replica 2 is " + cv);
//                            newUri = cr.insert(
//                                    buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider"),
//                                    cv
//                            );
                            break;
                        case QUERY_CUSTOM:      // QUERY_CUSTOM + ";;" + curNode.port + "==" + portToSend;
                            String oPort = msgRead[1];
                            if(oPort != curNode) { //origin port need not be set
//                                for (Map.Entry<String, String> map : localDB.entrySet()) {
//                                    Log.i(TAG, " map values which are in * :" + map.getKey() + ";;" + map.getValue());
//                                    String msgToOrigin = QUERY_HM_RESULT + ";;" + map.getKey() + ";;" + map.getValue();
//                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(oPort));
//                                    OutputStream outToAnother = socket.getOutputStream();
//                                    Log.i(TAG, "the port :" + oPort);
//                                    outToAnother.write(msgToOrigin.getBytes());
//                                    socket.close();
//                                }
//                                //changing to content provider
//                                String corValues = localDB.toString();
//                                String msgToSend = QUERY_STAR_RESULT + ";;" + corValues + "==" + oPort;
//                                Log.i(TAG, "Query Star result");
//                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
                                db = mDBhandler.getReadableDatabase();
                                String[] reqColumns2 = {DBHandler.COL_NAME_KEY, DBHandler.COL_NAME_VALUE};
                                cursor = db.query(DBHandler.TABLE_NAME, reqColumns2, null, null, null, null, null);
                                Log.i(TAG, "length of cursor :" + cursor.getColumnCount());
                                cursor.moveToFirst();
                                String msgToSend = QUERY_STAR_RESULT_NEW + ";;";
                                for (int move = 0; move < cursor.getCount(); move++) {
                                    String keyTemp = cursor.getString(cursor.getColumnIndex(DBHandler.COL_NAME_KEY));
                                    String valueTemp = cursor.getString(cursor.getColumnIndex(DBHandler.COL_NAME_VALUE));
                                    msgToSend += keyTemp  + "=" + valueTemp + ",";
                                    cursor.moveToNext();
                                }
                                msgToSend += "==" + oPort;
                                Log.i(TAG, "Query_one_succ_new: found key " + msgToSend);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
                            }
                            break;
                        case QUERY_STAR_RESULT_NEW:
                            String kvValues = msgRead[1];
                            String[] sp = kvValues.split(",");
                            for(String ele : sp) {
                                String x = ele.trim();
                                System.out.println(x);
                                String[] kvPairs = x.split("=");
                                if(kvPairs.length == 2) {
                                    System.out.println("result to be stored in container is :" + kvPairs[0] + "::" + kvPairs[1]);
                                    container.add(kvPairs[0] + ";;" + kvPairs[1]);
                                }
                            }
                            break;
                        case QUERY_ONE:      // QUERY_ONE + ";;" + key + ";;" + curPort + "==" + portToSend;
//                            synchronized (lockQuery) {
                                String key_one = msgRead[1];
                                String oPort_one = msgRead[2];
                                if (localDB.containsKey(key_one)) {
//                                    String value_one = localDB.get(key_one);
                                    db = mDBhandler.getReadableDatabase();
                                    String[] reqColumns1 = {DBHandler.COL_NAME_KEY, DBHandler.COL_NAME_VALUE};
                                    Cursor c = null;
                                    c = db.query(DBHandler.TABLE_NAME, reqColumns1, DBHandler.COL_NAME_KEY + "=" + "'" + key_one + "'", null, null, null, null);
                                    c.moveToFirst();
                                    String keyTemp = c.getString(c.getColumnIndex(DBHandler.COL_NAME_KEY));
                                    String valueTemp = c.getString(c.getColumnIndex(DBHandler.COL_NAME_VALUE));
                                    String msgToSend = QUERY_FOUND_KEY + ";;" + key_one + ";;" + valueTemp + "==" + oPort_one;
                                    Log.i(TAG, "Query_next: found key  :" + msgToSend);
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
                                } else { //send it to the corresponding port

                                }
  //                          }
                            break;
                        case QUERY_ONE_SUCC:    //QUERY_ONE_SUCC + ";;" + key + ";;" + curPort + "==" + (Integer.parseInt(succOfToSend) * 2);

                                String key_one_succ = msgRead[1];
                                String oPort_one_succ = msgRead[2];
                                Log.i(TAG, "Query_ONE_SUCC for key " + key_one_succ);
//                            if(replicaDB.containsKey(key_one_succ)) {
//                                String value_one_succ = replicaDB.get(key_one_succ);
//                                String msgToSend = QUERY_FOUND_KEY + ";;" + key_one_succ  + ";;" + value_one_succ + "==" + oPort_one_succ;
//                                Log.i(TAG, "Query_next: found key  :" + msgToSend);
//                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
//                            } else if(localDB.containsKey(key_one_succ)) {
//                                String value_one_succ = localDB.get(key_one_succ);
//                                String msgToSend = QUERY_FOUND_KEY + ";;" + key_one_succ  + ";;" + value_one_succ + "==" + oPort_one_succ;
//                                Log.i(TAG, "Query_next: found key  :" + msgToSend);
//                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
//                            }
                                db = mDBhandler.getReadableDatabase();
                                String[] reqColumns1 = {DBHandler.COL_NAME_KEY, DBHandler.COL_NAME_VALUE};
                                Cursor c =null;
                                c = db.query(DBHandler.TABLE_NAME, reqColumns1, DBHandler.COL_NAME_KEY + "=" + "'" + key_one_succ + "'", null, null, null, null);
//                                String value1;
//                                if (cursor.moveToFirst()) {
//                                    do {
//                                        String key1 = cursor.getString(cursor.getColumnIndex(DBHandler.COL_NAME_KEY));
//                                        value1 = cursor.getString(cursor.getColumnIndex(DBHandler.COL_NAME_VALUE));
//                                    } while (cursor.moveToNext());
//                                    String msgToSend = QUERY_FOUND_KEY + ";;" + key_one_succ + ";;" + value1 + "==" + oPort_one_succ;
//                                    Log.i(TAG, "Query_one_succ: found key  :" + msgToSend);
//                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
//                                }
//                                Log.i(TAG, "QUERY_ONE_SUCC value of cursor count " + cursor.getCount());
                                c.moveToFirst();
                            String keyTemp = c.getString(c.getColumnIndex(DBHandler.COL_NAME_KEY));
                            String valueTemp = c.getString(c.getColumnIndex(DBHandler.COL_NAME_VALUE));

                            String msgToSend = QUERY_FOUND_SUCC + ";;" + key_one_succ  + ";;" + valueTemp + "==" + oPort_one_succ;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
//                                for (int move = 0; move < cursor.getCount(); move++) {
//
//                                    Log.i(TAG, "Query_one_succ: found key  :" + msgToSend);
//
//                                    cursor.moveToNext();
//                                }

                            break;
                        case QUERY_FOUND_SUCC:
                            String c1 = msgRead[1];
                            String c2 = msgRead[2];
                            if(curKey.equals(c1)) {
                                Log.i(TAG, "got the key value in succ" + c1 + "::" + c2);
                                curKey ="";
                                String colnames[] ={"key","value"};
                                mCursor = new MatrixCursor(colnames);
                                mCursor.addRow(new String[] {c1, c2});
                                stopFlag = true;
                                Log.i(TAG, "Query found key in succ" + c1 + "::" + c2);
                            }
                            break;
                        case QUERY_FOUND_KEY: //QUERY_FOUND_KEY + ";;" + key_one  + ";;" + value_one + "==" + oPort_one;
                            //add to the array list
                            String column1 = msgRead[1];
                            String column2 = msgRead[2];
                            if(curKey.equals(column1)) {
                                Log.i(TAG, "got the key value " + column1 + "::" + column2);
                                curKey = "";
                                String colnames[] ={"key","value"};
                                mCursor = new MatrixCursor(colnames);

                                mCursor.addRow(new String[] {column1, column2});
                                stopFlag = true;
                            }
                            break;
                        case QUERY_HM_RESULT: // QUERY_HM_RESULT + ";;" + map.getKey() + ";;" + map.getValue() + "==" + oPort;
                            String result = msgRead[1] + ";;" + msgRead[2];
                            Log.i(TAG, "results stored in the container is " + result);
                            container.add(result);
                            break;
                        case QUERY_STAR_RESULT: //QUERY_STAR_RESULT + ";;" + corValues + "==" + oPort;
                            String hmValues = msgRead[1];
                            String res = hmValues.replaceAll("[\\[\\](){}]","");
                            Log.i(TAG, "star results of hashmap values " + res);
                            String[] kvSplit = res.split(",");
                            for(String ele : kvSplit) {
                                String x = ele.trim();
                                System.out.println(x);
                                String[] kvPairs = x.split("=");
                                if(kvPairs.length == 2) {
                                    System.out.println("result to be stored in container is :" + kvPairs[0] + "::" + kvPairs[1]);
                                    container.add(kvPairs[0] + ";;" + kvPairs[1]);
                                }
                            }
                            break;
                        case DELETE_IN_COR:
                            String key_to_delete = msgRead[1];
                            db = mDBhandler.getWritableDatabase();
                            localDB.remove(key_to_delete);
                            db.delete(DBHandler.TABLE_NAME, DBHandler.COL_NAME_KEY + "=" + "'" + key_to_delete + "'", null);
                            break;
                        case DELETE_IN_REPLICA_1: //DELETE_IN_REPLICA_1 + ";;" + key + "==" + (Integer.parseInt(replica1) * 2);
                            String key_to_delete_1 = msgRead[1];
                            db = mDBhandler.getWritableDatabase();
                            replicaDB.remove(key_to_delete_1);
                            db.delete(DBHandler.TABLE_NAME, DBHandler.COL_NAME_KEY + "=" + "'" + key_to_delete_1 + "'", null);
                            break;
                        case DELETE_IN_REPLICA_2: //DELETE_IN_REPLICA_1 + ";;" + key + "==" + (Integer.parseInt(replica1) * 2);
                            String key_to_delete_2 = msgRead[1];
                            db = mDBhandler.getWritableDatabase();
                            replicaDB.remove(key_to_delete_2);
                            db.delete(DBHandler.TABLE_NAME, DBHandler.COL_NAME_KEY + "=" + "'" + key_to_delete_2 + "'", null);
                            break;
                    }
                }
            } catch (IOException e) {
                Log.e("TAG", "Server Socket creation failed");
            }
            return null;
        }
    }
    /*
     format :   signalType;;msgToSEnd;;Seperatedwitsemicolon==Port
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                //print socket address
                Log.i(TAG, "got the socket address");
                String[] msgWithPort = msgs[0].split("==");  //to get the port address
                String remotePort = msgWithPort[1];
                String dataToSend = msgWithPort[0];
                Log.i("msgWithPort", msgWithPort[0]);
                if(remotePort != null) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                    OutputStream outToAnother = socket.getOutputStream();
                    Log.i("Port Name", remotePort);
                    outToAnother.write(dataToSend.getBytes());
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException :" + e);
            }

            return null;
        }
    }
    /*
    used to update the Local DB contents of the content provider
     */
    private class UpdateLocalDBTask extends AsyncTask<String, Void, Void> {
        String TAG = UpdateLocalDBTask.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {
            Log.i(TAG, "Update localDB class with msg :" + msgs);     //{1=one, 2=two, 3=three}
            String corValues = msgs[0];
            String res = corValues.replaceAll("[\\[\\](){}]","");
            Log.i(TAG, "cor values after removing the braces" + res);
            SQLiteDatabase db = mDBhandler.getWritableDatabase();;
            ContentValues cv;
            String[] sp = res.split(",");
            for(String ele : sp) {
                String x = ele.trim();
                System.out.println(x);
                String[] kvPairs = x.split("=");
                try {
                    String resNode = getNodeId(kvPairs[0]);
                    if(resNode.compareTo(curNode) == 0) {   //store only if they are equal
                        System.out.println("result to be stored in cor are :" + kvPairs[0] + "::" + kvPairs[1]);
                        localDB.put(kvPairs[0], kvPairs[1]);
                        cv = new ContentValues();
                        cv.put(DBHandler.COL_NAME_KEY, kvPairs[0]);
                        cv.put(DBHandler.COL_NAME_VALUE, kvPairs[1]);
//                        cv.put(DBHandler.COL_TYPE, TYPE_COR);       //setting as coordinator
                        long rowId1 = db.insertWithOnConflict(DBHandler.TABLE_NAME, null, cv, android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE);
                        Log.v("insert", "insert in the updateLocalDB Task " + cv.toString());
                        if (rowId1 == -1) {   //value already exists
                            Log.i("Conflict", "Error inserting values in DB");
                        } else {
                            Log.i(TAG, "success fully inserted in Local DB" + cv.toString());
                        }
                    }
                } catch (Exception e) {
                    Log.e(TAG, "Exception in Update local DB task : " + e);
                }

            }

//            if(msgs[0].contains("==")) {
//                String[] keyValueString = msgs[0].split("==");
//                SQLiteDatabase db = mDBhandler.getWritableDatabase();;
//                ContentValues cv;
//                for(String kv : keyValueString) {
//                    String[] kvArr = kv.split("::");
//                    localDB.put(kvArr[0], kvArr[1]);
//                    Log.i(TAG, "inserted this pair in localDB :" + kvArr[0] + "," + kvArr[1]);
//                    cv = new ContentValues();
//                    cv.put(DBHandler.COL_NAME_KEY, kvArr[0]);
//                    cv.put(DBHandler.COL_NAME_VALUE, kvArr[1]);
//                    cv.put(DBHandler.COL_TYPE, TYPE_COR);       //setting as coordinator
//                    long rowId1 = db.insertWithOnConflict(DBHandler.TABLE_NAME, null, cv, android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE);
//                    Log.v("insert", "insert since one node" + cv.toString());
//                    if (rowId1 == -1) {   //value already exists
//                        Log.i("Conflict", "Error inserting values in DB");
//                    } else {
//                        Log.i(TAG, "success fully inserted in Local DB" + cv.toString());
//                    }
//
//                }
//            }
            return null;
        }
    }
    /*
    used to update the replica 1 DB contents of the content provider
     */
    private class UpdateReplica1DBTask extends AsyncTask<String, Void, Void> {
        String TAG = UpdateLocalDBTask.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {
            Log.i(TAG, "Update ReplicaDB class with msg :" + msgs);     //{1=one, 2=two, 3=three}
            String repValues = msgs[0];
            String res = repValues.replaceAll("[\\[\\](){}]","");
            Log.i(TAG, " replica values after removing the braces" + res);
            SQLiteDatabase db = mDBhandler.getWritableDatabase();;
            ContentValues cv;
            String[] sp = res.split(",");
            for(String ele : sp) {
                String x = ele.trim();
                System.out.println(x);
                String[] kvPairs = x.split("=");
                if(kvPairs.length == 2) {
                    System.out.println("result to be stored in replica is :" + kvPairs[0] + "::" + kvPairs[1]);
                    replicaDB.put(kvPairs[0], kvPairs[1]);
                    cv = new ContentValues();
                    cv.put(DBHandler.COL_NAME_KEY, kvPairs[0]);
                    cv.put(DBHandler.COL_NAME_VALUE, kvPairs[1]);
//                    cv.put(DBHandler.COL_TYPE, TYPE_REP1);       //setting as coordinator
                    long rowId1 = db.insertWithOnConflict(DBHandler.TABLE_NAME, null, cv, android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE);
                    Log.v("insert", "insert since one node" + cv.toString());
                    if (rowId1 == -1) {   //value already exists
                        Log.i("Conflict", "Error inserting values in DB");
                    } else {
                        Log.i(TAG, "success fully inserted in replica DB" + cv.toString());
                    }
                }

            }
            return null;
        }
    }
    //helper functions
    /*
    @param: String: key value
    @return: Node where the key has to inserted
     */
    private String getNodeId(String key)  {
        String keyHash = "";
        try {
            keyHash = genHash(key);
            Log.i(TAG, "in gerNodeid generated hash is "+ key + " :: " + keyHash);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No argument exception in getNode ID " + e);
        }
        String insertNode = null;
//        boolean stop = false;
//        for(int i = 0; !stop && i < 5; i++) {   //check from starting
//            int cmp = keyHash.compareTo(hashList.get(i));      //if greater then that is the node we need to insert on
//            if(cmp > 0) {
//                insertNode = nodeList.get(i);
//                Log.i(TAG, "in getNode id inside if loop cmp value is " +  cmp + "::" + insertNode);
//                stop = true;
//            }
//        }
//        if(insertNode == null) {    //setting the last node as insert node if null
//            insertNode = nodeList.get(4);
//            Log.i(TAG, "in getNode id inside if insert node is null " + insertNode);
//        }

        int key_62 = keyHash.compareTo(hashList.get(0));
        int key_56 = keyHash.compareTo(hashList.get(1));
        int key_54 = keyHash.compareTo(hashList.get(2));
        int key_58 = keyHash.compareTo(hashList.get(3));
        int key_60 = keyHash.compareTo(hashList.get(4));
        Log.i(TAG, "compare vaues are 62 56 54 58 60 " + key_62 + " " + key_56 + " " + key_54 + " " + key_58 + " " + key_60 );
        if(key_62 <= 0 || key_60 > 0) {  //cornor case
            insertNode = "5562";
            Log.i(TAG, "getNodeId insert node 1:" + insertNode);
        } else if(key_62 > 0 && key_56 <= 0) {
            insertNode = nodeList.get(1);
            Log.i(TAG, "getNodeId insert node 2:" + insertNode);
        } else if(key_56 > 0 && key_54 <= 0) {
            insertNode = nodeList.get(2);
            Log.i(TAG, "getNodeId insert node 3:" + insertNode);
        } else if(key_54 > 0 && key_58 <= 0) {
            insertNode = nodeList.get(3);
            Log.i(TAG, "getNodeId insert node 4:" + insertNode);
        } else if(key_58 > 0 && key_60 <= 0) {
            insertNode = nodeList.get(4);
            Log.i(TAG, "getNodeId insert node 5:" + insertNode);
        }
        return insertNode;
    }
    /*
   Builds the URI for content resolver
    */
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
