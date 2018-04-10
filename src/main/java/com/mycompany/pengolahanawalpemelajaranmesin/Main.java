/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.pengolahanawalpemelajaranmesin;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.magicwerk.brownies.collections.BigList;

/**
 *
 * @author hadoop_user
 */
public class Main {

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/tawar", args[0], args[1]);

            ResultSet resultSet = connection.prepareStatement("select * from tawaran").executeQuery();

            final HashMap<String, Integer> job = new HashMap();
            job.put('"' + "admin." + '"', 0);
            job.put('"' + "blue-collar" + '"', 1);
            job.put('"' + "entrepreneur" + '"', 2);
            job.put('"' + "housemaid" + '"', 3);
            job.put('"' + "management" + '"', 4);
            job.put('"' + "retired" + '"', 5);
            job.put('"' + "self-employed" + '"', 6);
            job.put('"' + "services" + '"', 7);
            job.put('"' + "student" + '"', 8);
            job.put('"' + "technician" + '"', 9);
            job.put('"' + "unemployed" + '"', 10);
            job.put('"' + "unknown" + '"', 11);

            final HashMap<String, Integer> marital = new HashMap();
            marital.put('"' + "divorced" + '"', 0);
            marital.put('"' + "married" + '"', 1);
            marital.put('"' + "single" + '"', 2);
            marital.put('"' + "unknown" + '"', 3);

            final HashMap<String, Integer> education = new HashMap();
            education.put('"' + "basic.4y" + '"', 0);
            education.put('"' + "basic.6y" + '"', 1);
            education.put('"' + "basic.9y" + '"', 2);
            education.put('"' + "high.school" + '"', 3);
            education.put('"' + "illiterate" + '"', 4);
            education.put('"' + "professional.course" + '"', 5);
            education.put('"' + "university.degree" + '"', 6);
            education.put('"' + "unknown" + '"', 7);

            final HashMap<String, Integer> creditDefault = new HashMap();
            creditDefault.put('"' + "yes" + '"', 0);
            creditDefault.put('"' + "no" + '"', 1);
            creditDefault.put('"' + "unknown" + '"', 2);

            final HashMap<String, Integer> housing = new HashMap();
            housing.put('"' + "yes" + '"', 0);
            housing.put('"' + "no" + '"', 1);
            housing.put('"' + "unknown" + '"', 2);

            final HashMap<String, Integer> loan = new HashMap();
            loan.put('"' + "yes" + '"', 0);
            loan.put('"' + "no" + '"', 1);
            loan.put('"' + "unknown" + '"', 2);

            final HashMap<String, Integer> contact = new HashMap();
            contact.put('"' + "cellular" + '"', 0);
            contact.put('"' + "telephone" + '"', 1);

            final HashMap<String, Integer> months = new HashMap();
            months.put('"' + "january" + '"', 0);
            months.put('"' + "february" + '"', 1);
            months.put('"' + "march" + '"', 2);
            months.put('"' + "april" + '"', 3);
            months.put('"' + "may" + '"', 4);
            months.put('"' + "june" + '"', 5);
            months.put('"' + "july" + '"', 6);
            months.put('"' + "august" + '"', 7);
            months.put('"' + "september" + '"', 8);
            months.put('"' + "october" + '"', 9);
            months.put('"' + "november" + '"', 10);
            months.put('"' + "december" + '"', 11);

            final HashMap<String, Integer> days = new HashMap();
            days.put('"' + "sun" + '"', 0);
            days.put('"' + "mon" + '"', 1);
            days.put('"' + "tue" + '"', 2);
            days.put('"' + "wed" + '"', 3);
            days.put('"' + "thu" + '"', 4);
            days.put('"' + "fri" + '"', 5);
            days.put('"' + "sat" + '"', 6);

            final HashMap<String, Integer> poutcome = new HashMap();
            poutcome.put('"' + "failure" + '"', 0);
            poutcome.put('"' + "nonexistent" + '"', 0);
            poutcome.put('"' + "success" + '"', 0);

            final HashMap<String, Integer> y = new HashMap();
            y.put('"' + "yes" + '"', 0);
            y.put('"' + "no" + '"', 1);

            final File file = new File("formattedData.csv");
            final BigList<String> stringBigList = new BigList(40000);

            int counter = 0;

            while (resultSet.next()) {
                
                if(counter%5000 == 0){
                    System.out.println("Iterasi Ke: "+counter);
                }

                final String data = resultSet.getInt("age") + ";" + job.get(resultSet.getString("job")) + ";" + marital.get(resultSet.getString("marital")) + ";" + education.get(resultSet.getString("education")) + ";" + creditDefault.get(resultSet.getString("default")) + ";" + housing.get(resultSet.getString("housing")) + ";" + loan.get(resultSet.getString("loan")) + ";" + contact.get(resultSet.getString("contact")) + ";" + months.get(resultSet.getString("month")) + ";" + days.get(resultSet.getString("day_of_week")) + ";" + resultSet.getDouble("duration") + ";" + resultSet.getDouble("campaign") + ";" + resultSet.getDouble("pdays") + ";" + resultSet.getDouble("previous") + ";" + poutcome.get(resultSet.getString("poutcome")) + ";" + resultSet.getDouble("emp_var_rate") + ";" + resultSet.getDouble("cons_price_idx") + ";" + resultSet.getDouble("cons_conf_idx") + ";" + resultSet.getDouble("euribor3m") + ";" + resultSet.getDouble("nr_employed") + ";" + y.get(resultSet.getString("y"));
                stringBigList.add(data);

                //<editor-fold defaultstate="collapsed" desc="Database Insertion Point">
//                final PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO tawarandua (age, job, marital, education, default, housing,loan,	contact,month, day_of_week, duration, campaign, pdays,previous, poutcome, emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m,nr_employed, y) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" );
//                
//                preparedStatement.setInt(1, resultSet.getInt("age"));
//                preparedStatement.setInt(2, job.get(resultSet.getString("job")));
//                preparedStatement.setInt(3, marital.get(resultSet.getString("marital")));
//                preparedStatement.setInt(4, education.get(resultSet.getString("education")));
//                preparedStatement.setInt(5, creditDefault.get(resultSet.getString("default")));
//                preparedStatement.setInt(6, housing.get(resultSet.getString("housing")));
//                preparedStatement.setInt(7, loan.get(resultSet.getString("loan")));
//                preparedStatement.setInt(8, contact.get(resultSet.getString("contact")));
//                preparedStatement.setInt(9, months.get(resultSet.getString("month")));
//                preparedStatement.setInt(10, days.get(resultSet.getString("day_of_week")));
//                preparedStatement.setDouble(11, resultSet.getDouble("duration"));
//                preparedStatement.setDouble(12, resultSet.getDouble("campaign"));
//                preparedStatement.setDouble(13, resultSet.getDouble("pdays"));
//                preparedStatement.setDouble(14, resultSet.getDouble("previous"));
//                preparedStatement.setInt(15, poutcome.get(resultSet.getString("poutcome")));
//                preparedStatement.setDouble(16, resultSet.getDouble("emp_var_rate"));
//                preparedStatement.setDouble(17, resultSet.getDouble("cons_price_idx"));
//                preparedStatement.setDouble(18, resultSet.getDouble("cons_conf_idx"));
//                preparedStatement.setDouble(19, resultSet.getDouble("euribor3m"));
//                preparedStatement.setDouble(20, resultSet.getDouble("nr_employed"));
//                preparedStatement.setInt(21, y.get(resultSet.getString("y")));
//                
//                preparedStatement.executeUpdate();
//</editor-fold>
                counter++;
            }

            FileUtils.writeLines(file, stringBigList);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
