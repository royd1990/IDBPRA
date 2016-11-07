package de.tuberlin.dima.minidb.warm_up;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by royd1990 on 10/31/16.
 */
public class SortImpl implements Sort{
    public SortImpl() {
    }

    @Override
    public ArrayList<Comparable> sort(ArrayList<Comparable> table) {
        table.sort(new Comparator<Comparable>(){
            @Override
            public int compare(Comparable c, Comparable t1) {
                return c.compareTo(t1);
            }
        });

        return table;
    }
}
