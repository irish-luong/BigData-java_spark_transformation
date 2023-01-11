package com.max.model;


import lombok.Data;

@Data
public class Food {

    private String food_name;
    private String scientific_name;
    private String group;
    private String sub_group;


    public static String[] getFields() {
        return new String[] {
                "group",
                "food_name",
                "sub_group",
                "scientific_name"
        };
    }

}
