package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by prakashn on 23/04/15.
 */

import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.content.Context;
import android.util.Log;

public class DBHandler extends SQLiteOpenHelper {
    /*
    @author prakashn
    */
    public static final String TAG = DBHandler.class.getSimpleName();
    public static final String DATABASE_NAME = "chat.db";
    public static final int DATABASE_VERSION = 1;


    public static final String TABLE_NAME = "chat";
    public static final String COL_NAME_KEY = "key";
    public static final String COL_NAME_VALUE = "value";
    //inserting new columns for detection of coordinator and replica detection
    public static final String COL_TYPE = "type";   // types : cor, rep1, rep2
    public static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + "( " +
            COL_NAME_KEY + " text PRIMARY KEY, " + COL_NAME_VALUE + " text );";
//            COL_NAME_VALUE + " text , " +
//            COL_TYPE + " text );";  // types : cor, rep1, rep2
    public DBHandler(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        try {
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
            db.execSQL(CREATE_TABLE);
            Log.i(TAG, "created table with query : " + CREATE_TABLE);
        } catch (SQLException e) {
            Log.v(TAG, "SQLException in oncreate");

        }
        Log.v(TAG, "database created");

    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        //not Implemented as there is not upgrade of DB
    }

    public void deleteTable(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
    }
}
