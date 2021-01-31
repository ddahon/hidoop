package outils;
import ordo.*;
import formats.Format;
import application.MyMapReduce;

class TestIntegration {

    public static void main(String args[]) {
        Format.Type type = Format.Type.LINE;
        String name = "";

        Job job = new Job();
        job.setInputFormat(type);
        job.setInputFname(name);
        job.setNodesInfos();
        job.startJob(new MyMapReduce());
    }
}