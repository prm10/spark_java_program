package datrain;

import java.util.Comparator;

public class Min_Heap {
    public kv[] result = null;
    public int size;

    Min_Heap(int k) {
        result = new kv[k];
        size = 0;
    }

    public void add(String k, double v) {
        if (size < result.length) {//数组没满，不用建堆
            result[size] = new kv(k, v);
            size++;
            if(size==result.length){
                createHeap();
            }
        } else {
            if (v > result[0].value) {
                insert(k, v);
            }
        }
    }

    void createHeap() {
        for (int i = 1; i < result.length; i++) {
            int child = i;
            int parent = (i - 1) / 2;
            kv temp = result[i];
            while (parent >= 0 && child != 0 && result[parent].value > temp.value) {
                result[child] = result[parent];
                child = parent;
                parent = (parent - 1) / 2;
            }
            result[child] = temp;
        }
    }

    void insert(String k, double value) {
        result[0].value = value;
        result[0].key = k;
        int parent = 0;

        while (parent < result.length) {
            int lchild = 2 * parent + 1;
            int rchild = 2 * parent + 2;
            int minIndex = parent;
            if (lchild < result.length && result[parent].value > result[lchild].value) {
                minIndex = lchild;
            }
            if (rchild < result.length && result[minIndex].value > result[rchild].value) {
                minIndex = rchild;
            }
            if (minIndex == parent) {
                break;
            } else {
                kv temp = result[parent];
                result[parent] = result[minIndex];
                result[minIndex] = temp;
                parent = minIndex;
            }
        }
    }

    public class kv {
        public String key;
        public double value;

        kv(String k, double v) {
            key = k;
            value = v;
        }
    }

    public void sort() {
        if(size<result.length){
            kv[] t=new kv[size];
            for(int i=0;i<size;i++) t[i] = result[i];
            result=t;
        }
        java.util.Arrays.sort(result, new MyComparator());
    }

    class MyComparator implements Comparator {
        public int compare(Object o1, Object o2) {
            kv u1 = (kv) o1;
            kv u2 = (kv) o2;
            if (u1.value > u2.value) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    public static void main(String[] args) {
        double a[] = {4.2e-10, 3.2, 5, 1, 2, .8, 9, 10e-10};
        String b[] = {"a", "b", "c", "d", "e", "f", "g", "h"};
        Min_Heap heap = new Min_Heap(10);
        for (int i = 0; i < a.length; i++) {
            heap.add(b[i], a[i]);
        }
        for (int i = 0; i < heap.size; i++) {
            System.out.println(heap.result[i].key + ":" + heap.result[i].value);
        }
        System.out.println("heap size: " + heap.size);
        heap.sort();
        double weight=1;
        Min_Heap.kv item_entry = heap.result[0];
        String item_list = item_entry.key + ":" + item_entry.value / weight;//rec_item_id,score
        for (int i = 1; i < heap.size; i++) {
            item_entry = heap.result[i];
            item_list += ";" + item_entry.key + ":" + item_entry.value / weight;
        }
        System.out.println(item_list);
    }
}