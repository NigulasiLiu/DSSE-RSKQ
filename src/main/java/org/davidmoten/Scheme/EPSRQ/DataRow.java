package org.davidmoten.Scheme.EPSRQ;

public class DataRow {
    public int fileID;
    public long pointX;
    public long pointY;
    public String[] keywords;

    public DataRow(int fileID, long pointX, long pointY, String[] keywords) {
        this.fileID = fileID;
        this.pointX = pointX;
        this.pointY = pointY;
        this.keywords = keywords;
    }
}
