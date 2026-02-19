package com.zenz.kvstore;

public interface Operation {
    int id();

    OperationType type();

    static Operation fromLine(String line) {
        String[] components = line.strip().split(" ");
        System.out.println("Components: ");
        for (String comp : components) {
            System.out.print(comp);
        }
        System.out.println();
        System.out.println("<>" + components[1]);
        int id = Integer.parseInt(components[0]);
        OperationType type = OperationType.valueOf(components[1]);

        return switch (type) {
            case PUT -> PutOperation.fromLine(id, components);
            case GET -> GetOperation.fromLine(id, components);
        };
    }
}