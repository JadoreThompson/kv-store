package com.zenz.kvstore;


public class Log {
    public final int id;
    public final OperationType operation;
    public final String message;

    public Log(int id, OperationType operation, String message) {
        this.id = id;
        this.operation = operation;
        this.message = message;
    }

    public static Log fromLine(String line) {
        String[] components = line.strip().split(" ");
        int id = Integer.parseInt(components[0]);
        OperationType operation = OperationType.valueOf(components[1]);

        StringBuilder builder = new StringBuilder();
        for (int i = 2; i < components.length; i++) {
            builder.append(components[i] + " ");
        }
//        String message = components[2];
//        return new Log(id, operation, message);
        return new Log(id, operation, builder.toString().strip());
    }
}


