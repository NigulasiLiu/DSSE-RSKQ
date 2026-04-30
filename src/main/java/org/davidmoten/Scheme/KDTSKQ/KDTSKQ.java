package org.davidmoten.Scheme.KDTSKQ;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.function.Function;

public class KDTSKQ {

    // 系统参数
    public int L;                                // 每个分区允许的最大大小
    public byte[] Key;                           // 系统密钥
    public Function<byte[], byte[]> H1;          // 哈希函数 H1
    public Function<byte[], byte[]> H2;          // 哈希函数 H2
    public Map<String, byte[]> EDB;              // 加密数据库
    public Map<String, List<Long>> LocalTree;    // 更改为存储整数的 map
    public List<List<Integer>> ClusterFlist;     // 分区文件列表
    public List<List<String>> ClusterKlist;      // 分区关键词列表
    public Map<String, byte[]> KeywordToSK;      // 每个关键词对应的 OTP 密钥
    public int BsLength;                         // Bitmap 长度
    public int[] LocalPosition = new int[2];     // 查询范围对应的分区位置
    public List<String> Flags;                   // 标记需要查询的边界（左边界 "l"，右边界 "r"）
    public List<String> FlagEmpty;               // 标记查询结果是否为空

    private KDTSKQ() {
        // 私有构造器，通过 Setup 初始化
    }

    // ---------------------------------------------------------
    // Setup 初始化系统参数
    // ---------------------------------------------------------
    public static KDTSKQ Setup(int L) {
        KDTSKQ sp = new KDTSKQ();

        // 生成随机密钥
        byte[] key = new byte[16];
        new SecureRandom().nextBytes(key);
        sp.Key = key;
        sp.L = L;
        sp.BsLength = L;

        // 定义 H1 和 H2 (使用 JDK 22 var lambda 参数)
        sp.H1 = (var data) -> {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                return digest.digest(data);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        };

        sp.H2 = (var data) -> {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                return digest.digest(data);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        };

        sp.EDB = new HashMap<>();
        sp.LocalTree = new HashMap<>();
        sp.ClusterFlist = new ArrayList<>();
        sp.ClusterKlist = new ArrayList<>();
        sp.KeywordToSK = new HashMap<>();
        sp.Flags = new ArrayList<>();
        sp.FlagEmpty = new ArrayList<>();

        return sp;
    }

    // ---------------------------------------------------------
    // 核心模块 2: 索引构建 (BuildIndex)
    // ---------------------------------------------------------
    public void buildIndex(Map<String, List<Integer>> invertedIndex, List<String> keywords) {
        List<Integer> currentGroup = new ArrayList<>();
        List<String> currentKlist = new ArrayList<>();
        List<List<Integer>> clusterFlist = new ArrayList<>();
        List<List<String>> clusterKlist = new ArrayList<>();

        for (int i = 0; i < keywords.size(); i++) {
            String keyword = keywords.get(i);
            List<Integer> postings = invertedIndex.getOrDefault(keyword, new ArrayList<>());

            if (currentGroup.size() + postings.size() < this.L) {
                currentGroup.addAll(postings);
                currentKlist.add(keyword);

                encryptAndStore(keyword, currentGroup);

                if (i == keywords.size() - 1) {
                    clusterFlist.add(new ArrayList<>(currentGroup));
                    clusterKlist.add(new ArrayList<>(currentKlist));
                }
            } else {
                clusterFlist.add(new ArrayList<>(currentGroup));
                clusterKlist.add(new ArrayList<>(currentKlist));

                currentGroup = new ArrayList<>(postings);
                currentKlist = new ArrayList<>();
                currentKlist.add(keyword);

                encryptAndStore(keyword, currentGroup);

                if (i == keywords.size() - 1) {
                    clusterFlist.add(new ArrayList<>(currentGroup));
                    clusterKlist.add(new ArrayList<>(currentKlist));
                }
            }
        }

        this.ClusterFlist = clusterFlist;
        this.ClusterKlist = clusterKlist;

        buildLocalTree(clusterKlist);
    }

    private void buildLocalTree(List<List<String>> clusterKlist) {
        List<String[]> genList = new ArrayList<>();
        for (List<String> klist : clusterKlist) {
            if (!klist.isEmpty()) {
                genList.add(new String[]{klist.get(0), klist.get(klist.size() - 1)});
            }
        }

        int clusterHeight = (int) Math.ceil(Math.log(genList.size()) / Math.log(2));
        String padding = genList.get(genList.size() - 1)[1];

        while (genList.size() < (int) Math.pow(2, clusterHeight)) {
            genList.add(new String[]{padding, padding});
        }

        Map<String, List<Long>> localTree = new HashMap<>();
        for (int i = clusterHeight; i >= 0; i--) {
            int nodesInLevel = (int) Math.pow(2, i);
            for (int j = 0; j < nodesInLevel; j++) {
                String tempKey = String.format("%" + (i + 1) + "s", Integer.toBinaryString(j)).replace(' ', '0');
                if (i == clusterHeight) {
                    long leftv = Long.parseLong(genList.get(j)[0]);
                    long rightv = Long.parseLong(genList.get(j)[1]);
                    List<Long> values = new ArrayList<>();
                    values.add(leftv);
                    values.add(rightv);
                    localTree.put(tempKey, values);
                } else {
                    List<Long> leftChild = localTree.get(tempKey + "0");
                    List<Long> rightChild = localTree.get(tempKey + "1");
                    List<Long> values = new ArrayList<>();
                    values.add(leftChild.get(0));
                    values.add(rightChild.get(1));
                    localTree.put(tempKey, values);
                }
            }
        }
        this.LocalTree = localTree;
    }

    private void encryptAndStore(String keyword, List<Integer> postings) {
        byte[] bitmap = generateBitmap(postings);
        byte[] otpKey = this.H1.apply(keyword.getBytes());

        byte[] encryptedBitmap = xorBytesWithPadding(bitmap, otpKey, this.L);
        String hashedKey = bytesToHex(this.H1.apply(keyword.getBytes()));

        this.KeywordToSK.put(hashedKey, otpKey);
        this.EDB.put(hashedKey, encryptedBitmap);
    }

    // --------------------------
    // Update 方法修改（适配新入参+红黑树插入）
    // --------------------------
    public void update(String w, List<BigInteger> docID) throws Exception {
        int P_K = searchTree(w);

        if (P_K < 0 || P_K >= this.ClusterKlist.size()) {
            throw new Exception(String.format("关键词[%s]分区索引P_K=%d无效，超出范围", w, P_K));
        }

        int P_F = P_K;
        if (P_F < 0 || P_F >= this.ClusterFlist.size()) {
            throw new Exception(String.format("文件分区索引P_F=%d无效，超出范围", P_F));
        }

        RBTree rbtree = new RBTree();
        for (BigInteger id : docID) {
            rbtree.insert(id.intValue());
        }

        modifyFunction(this.ClusterFlist.get(P_F), rbtree);
    }

    private void modifyFunction(List<Integer> fileList, RBTree rbtree) {
        class TraverseAction {
            void traverse(RBNode node) {
                if (node == rbtree.NIL) return;
                traverse(node.left);
                fileList.add(node.value);
                traverse(node.right);
            }
        }
        new TraverseAction().traverse(rbtree.root);
    }

    // ---------------------------------------------------------
    // 红黑树相关类
    // ---------------------------------------------------------
    public static class RBNode {
        int value;
        boolean color; // true: 红色, false: 黑色
        RBNode left, right, parent;

        public RBNode() {}

        public RBNode(boolean color) {
            this.color = color;
        }
    }

    public static class RBTree {
        RBNode root;
        RBNode NIL;

        public RBTree() {
            this.NIL = new RBNode(false);
            this.root = this.NIL;
        }

        public void insert(int value) {
            RBNode newNode = new RBNode();
            newNode.value = value;
            newNode.color = true;
            newNode.left = this.NIL;
            newNode.right = this.NIL;

            RBNode parent = null;
            RBNode current = this.root;

            while (current != this.NIL) {
                parent = current;
                if (newNode.value < current.value) {
                    current = current.left;
                } else {
                    current = current.right;
                }
            }
            newNode.parent = parent;
            if (parent == null) {
                this.root = newNode;
            } else if (newNode.value < parent.value) {
                parent.left = newNode;
            } else {
                parent.right = newNode;
            }

            if (newNode.parent == null) {
                newNode.color = false;
                return;
            }
            if (newNode.parent.parent == null) {
                return;
            }

            fixInsert(newNode);
        }

        private void fixInsert(RBNode z) {
            while (z.parent != null && z.parent.color) {
                if (z.parent == z.parent.parent.left) {
                    RBNode y = z.parent.parent.right;
                    if (y != null && y.color) {
                        z.parent.color = false;
                        y.color = false;
                        z.parent.parent.color = true;
                        z = z.parent.parent;
                    } else {
                        if (z == z.parent.right) {
                            z = z.parent;
                            leftRotate(z);
                        }
                        z.parent.color = false;
                        z.parent.parent.color = true;
                        rightRotate(z.parent.parent);
                    }
                } else {
                    RBNode y = z.parent.parent.left;
                    if (y != null && y.color) {
                        z.parent.color = false;
                        y.color = false;
                        z.parent.parent.color = true;
                        z = z.parent.parent;
                    } else {
                        if (z == z.parent.left) {
                            z = z.parent;
                            rightRotate(z);
                        }
                        z.parent.color = false;
                        z.parent.parent.color = true;
                        leftRotate(z.parent.parent);
                    }
                }
                if (z == this.root) {
                    break;
                }
            }
            this.root.color = false;
        }

        private void leftRotate(RBNode x) {
            RBNode y = x.right;
            x.right = y.left;
            if (y.left != this.NIL) {
                y.left.parent = x;
            }
            y.parent = x.parent;
            if (x.parent == null) {
                this.root = y;
            } else if (x == x.parent.left) {
                x.parent.left = y;
            } else {
                x.parent.right = y;
            }
            y.left = x;
            x.parent = y;
        }

        private void rightRotate(RBNode y) {
            RBNode x = y.left;
            y.left = x.right;
            if (x.right != this.NIL) {
                x.right.parent = y;
            }
            x.parent = y.parent;
            if (y.parent == null) {
                this.root = x;
            } else if (y == y.parent.right) {
                y.parent.right = x;
            } else {
                y.parent.left = x;
            }
            x.right = y;
            y.parent = x;
        }
    }

    // ---------------------------------------------------------
    // 查询与解密模块
    // ---------------------------------------------------------
    public List<String> genToken(String[] queryRange) throws Exception {
        this.FlagEmpty.clear();
        this.Flags.clear();
        this.LocalPosition = new int[2];

        int p1 = searchTree(queryRange[0]);
        int p2 = searchTree(queryRange[1]);

        if (p1 > p2 + 1) {
            return new ArrayList<>();
        }
        this.LocalPosition[0] = p1;
        this.LocalPosition[1] = p2;

        List<List<String>> localCluster = this.ClusterKlist.subList(p1, p2 + 1);

        String startOfLocal = localCluster.get(0).get(0);
        List<String> endClusterList = localCluster.get(localCluster.size() - 1);
        String endOfLocal = endClusterList.get(endClusterList.size() - 1);

        if (queryRange[0].equals(startOfLocal) && queryRange[1].equals(endOfLocal)) {
            return new ArrayList<>();
        }

        List<String> serverTokens = new ArrayList<>();

        if (!queryRange[0].equals(startOfLocal)) {
            int tempIndex = indexOf(localCluster.get(0), queryRange[0]) - 1;
            if (tempIndex < 0) {
                int queryRangeInt = Integer.parseInt(queryRange[0]);
                int[] localClusterInt = localCluster.get(0).stream().mapToInt(Integer::parseInt).toArray();
                tempIndex = binarySearchClosest(localClusterInt, queryRangeInt, true);
            }
            String tempToken = localCluster.get(0).get(tempIndex);
            this.FlagEmpty.add(tempToken);
            serverTokens.add(tempToken);
            this.Flags.add("l");
        }

        if (!queryRange[1].equals(endOfLocal)) {
            List<String> lastList = localCluster.get(localCluster.size() - 1);
            int tempIndex = indexOf(lastList, queryRange[1]);
            if (tempIndex < 0) {
                int queryRangeInt = Integer.parseInt(queryRange[1]);
                int[] localClusterInt = lastList.stream().mapToInt(Integer::parseInt).toArray();
                tempIndex = binarySearchClosest(localClusterInt, queryRangeInt, false);
            }
            String tempToken = lastList.get(tempIndex);
            this.FlagEmpty.add(tempToken);
            serverTokens.add(tempToken);
            this.Flags.add("r");
        }

        if (p1 == p2 && this.FlagEmpty.size() == 2 && this.FlagEmpty.get(0).equals(this.FlagEmpty.get(1))) {
            return new ArrayList<>();
        }

        List<String> hashedTokens = new ArrayList<>();
        for (String token : serverTokens) {
            hashedTokens.add(bytesToHex(this.H1.apply(token.getBytes())));
        }

        return hashedTokens;
    }

    public List<byte[]> searchTokens(List<String> tokens) {
        List<byte[]> searchResult = new ArrayList<>();
        for (String token : tokens) {
            if (this.EDB.containsKey(token)) {
                searchResult.add(this.EDB.get(token));
            } else {
                System.out.println("Token not found in EDB: " + token);
            }
        }
        return searchResult;
    }

    public List<Integer> localSearch(List<byte[]> searchResult, List<String> tokens) {
        List<List<Integer>> clusterFlist = this.ClusterFlist;
        List<Integer> finalResult = new ArrayList<>();
        int p1 = this.LocalPosition[0];
        int p2 = this.LocalPosition[1];

        if (searchResult.isEmpty()) {
            for (int i = p1; i <= p2; i++) {
                finalResult.addAll(clusterFlist.get(i));
            }
            return finalResult;
        }

        int clusterSizeP1 = this.ClusterFlist.get(p1).size();
        String fullOneStr = "1".repeat(clusterSizeP1) + "0".repeat(Math.max(0, this.BsLength - clusterSizeP1));
        byte[] fullOneBytes = fullOneStr.getBytes();

        if (searchResult.size() == 2) {
            byte[] decLeft = xorBytesWithPadding(searchResult.get(0), this.KeywordToSK.get(tokens.get(0)), this.L);
            byte[] decRight = xorBytesWithPadding(searchResult.get(1), this.KeywordToSK.get(tokens.get(1)), this.L);

            if (p1 == p2) {
                byte[] compBitmap = xorBytesWithPadding(decLeft, decRight, this.L);
                finalResult.addAll(parseFileID_for_01(compBitmap, clusterFlist.get(p1)));
            } else {
                byte[] leftBitmap = xorBytesWithPadding(decLeft, fullOneBytes, this.L);
                finalResult.addAll(parseFileID_for_01(leftBitmap, clusterFlist.get(p1)));

                byte[] rightBitmap = decRight;
                finalResult.addAll(parseFileID(rightBitmap, clusterFlist.get(p2)));

                for (int i = p1 + 1; i < p2; i++) {
                    finalResult.addAll(clusterFlist.get(i));
                }
            }
        } else if (searchResult.size() == 1) {
            byte[] decResult = xorBytesWithPadding(searchResult.get(0), this.KeywordToSK.get(tokens.get(0)), this.L);

            if (this.Flags.contains("l")) {
                byte[] leftBitmap = xorBytesWithPadding(decResult, fullOneBytes, this.L);
                finalResult.addAll(parseFileID_for_01(leftBitmap, clusterFlist.get(p1)));
                for (int i = p1 + 1; i <= p2; i++) {
                    finalResult.addAll(clusterFlist.get(i));
                }
            }
            if (this.Flags.contains("r")) {
                byte[] rightBitmap = decResult;
                finalResult.addAll(parseFileID(rightBitmap, clusterFlist.get(p2)));
                for (int i = p1; i < p2; i++) {
                    finalResult.addAll(clusterFlist.get(i));
                }
            }
        }
        return finalResult;
    }

    private int searchTree(String queryValue) throws Exception {
        long queryValueInt = Long.parseLong(queryValue);
        String node = "0";

        List<Long> nodeValue = this.LocalTree.get(node);
        if (nodeValue == null) {
            throw new Exception("无法找到节点 " + node);
        }
        if (queryValueInt > nodeValue.get(1) || queryValueInt < nodeValue.get(0)) {
            throw new Exception("queryValueInt " + queryValueInt + " 在范围之外");
        }

        int height = (int) Math.ceil(Math.log(this.ClusterFlist.size()) / Math.log(2));
        for (int i = 0; i < height; i++) {
            long valuelr = this.LocalTree.get(node + "0").get(1);
            long valuerl = this.LocalTree.get(node + "1").get(0);

            if (queryValueInt > valuelr) {
                node += "1";
            } else if (queryValueInt < valuerl) {
                node += "0";
            }
        }

        List<Long> valuelf = this.LocalTree.get(node);
        valuelf.set(0, valuelf.get(0) + 1); // 模拟 Go 中的 valuelf[0] += 1

        return (int) Long.parseLong(node, 2);
    }

    // ---------------------------------------------------------
    // 工具/辅助模块
    // ---------------------------------------------------------
    private byte[] generateBitmap(List<Integer> group) {
        int remainingLength = Math.max(0, this.L - group.size());
        String bitString = "1".repeat(group.size()) + "0".repeat(remainingLength);
        return bitString.getBytes();
    }

    private byte[] xorBytesWithPadding(byte[] a, byte[] b, int bytelen) {
        if (bytelen <= 0) return new byte[0];
        if (a.length == b.length && Arrays.equals(a, b)) return a;

        byte[] truncatedA = new byte[bytelen];
        byte[] truncatedB = new byte[bytelen];

        System.arraycopy(a, Math.max(0, a.length - bytelen), truncatedA, Math.max(0, bytelen - a.length), Math.min(a.length, bytelen));
        System.arraycopy(b, Math.max(0, b.length - bytelen), truncatedB, Math.max(0, bytelen - b.length), Math.min(b.length, bytelen));

        byte[] result = new byte[bytelen];
        for (int i = 0; i < bytelen; i++) {
            result[i] = (byte) (truncatedA[i] ^ truncatedB[i]);
        }
        return result;
    }

    private List<Integer> parseFileID_for_01(byte[] bitmap, List<Integer> dbList) {
        List<Integer> result = new ArrayList<>();
        int dblen = dbList.size();
        for (int byteIndex = 0; byteIndex < bitmap.length; byteIndex++) {
            // 在 Go 中，某些异或后结果直接变为了 1 这个值，故适配 byte 取值
            if (bitmap[byteIndex] == 1 && byteIndex < dblen) {
                result.add(dbList.get(byteIndex));
            }
        }
        return result;
    }

    private List<Integer> parseFileID(byte[] bitmap, List<Integer> dbList) {
        List<Integer> result = new ArrayList<>();
        StringBuilder bitString = new StringBuilder();

        for (byte b : bitmap) {
            if (b >= 32 && b <= 126) {
                bitString.append((char) b);
            } else {
                bitString.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
            }
        }

        String finalBitString = bitString.toString();
        if (finalBitString.length() > this.L) {
            finalBitString = finalBitString.substring(finalBitString.length() - this.L);
        }

        for (int i = 0; i < finalBitString.length(); i++) {
            if (i >= dbList.size()) break;
            if (finalBitString.charAt(i) == '1') {
                result.add(dbList.get(i));
            }
        }
        return result;
    }

    private int indexOf(List<String> slice, String value) {
        return slice.indexOf(value);
    }

    private int binarySearchClosest(int[] slice, int value, boolean findLarger) {
        int low = 0, high = slice.length - 1;
        int closest = -1;

        while (low <= high) {
            int mid = (low + high) / 2;
            if (slice[mid] == value) return mid;

            if (findLarger) {
                if (slice[mid] > value) {
                    closest = mid;
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            } else {
                if (slice[mid] < value) {
                    closest = mid;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
        }
        return Math.max(closest, 0);
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
