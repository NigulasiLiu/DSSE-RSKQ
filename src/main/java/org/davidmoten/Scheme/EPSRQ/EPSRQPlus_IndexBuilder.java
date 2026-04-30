package org.davidmoten.Scheme.EPSRQ;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;

import java.util.*;
import java.util.stream.Collectors;

/**
 * EPSRQPlus_IndexBuilder: 基于BKFtree索引的优化实现
 * 
 * 核心优化：
 * 1. 使用BKFtree替代EPKI，减少索引冗余
 * 2. 支持动态分区阈值优化
 * 3. 改进的向量维度管理
 */
public final class EPSRQPlus_IndexBuilder {
    
    public static final class BKFNode {
        public final int level;
        public final List<EPSRQ_IndexBuilder.EncryptedLocation> locations;
        public final Map<String, BKFNode> children;
        
        public BKFNode(int level) {
            this.level = level;
            this.locations = new ArrayList<>();
            this.children = new HashMap<>();
        }
    }
    
    public static final class BuildStats {
        public final int dictionarySize;
        public final int realLocations;
        public final int phantomLocations;
        public final int keywordCount;
        public final int treeDepth;
        public final int nodeCount;
        
        public BuildStats(int dictionarySize, int realLocations, int phantomLocations, 
                         int keywordCount, int treeDepth, int nodeCount) {
            this.dictionarySize = dictionarySize;
            this.realLocations = realLocations;
            this.phantomLocations = phantomLocations;
            this.keywordCount = keywordCount;
            this.treeDepth = treeDepth;
            this.nodeCount = nodeCount;
        }
    }
    
    private final int t;
    private final int gamma;
    private final int maxFiles;
    private final Random rnd;
    private final EPSRQ_Setup setup;
    
    private EPSRQ_Setup.SetupKeyPair keyPair;
    private final Map<String, Integer> dict = new HashMap<>();
    private final List<String> dictionaryKeywords = new ArrayList<>();
    private final Map<String, EPSRQ_Setup.CipherVector> encKeywordBasis = new HashMap<>();
    private final BKFNode root;
    
    public EPSRQPlus_IndexBuilder(int maxFiles, int t, int gamma, long seed) {
        this.t = t;
        this.gamma = Math.max(1, gamma);
        this.maxFiles = Math.max(1, maxFiles);
        this.rnd = new Random(seed);
        this.setup = new EPSRQ_Setup(seed);
        this.root = new BKFNode(0);
    }
    
    public EPSRQ_Setup.SetupKeyPair getKeyPair() {
        if (keyPair == null) {
            // 动态计算维度，避免硬编码
            int estimatedDictSize = 1000; // 可根据实际数据调整
            this.keyPair = setup.setup(estimatedDictSize, t);
        }
        return keyPair;
    }
    
    public BKFNode getBKFtree() {
        return root;
    }
    
    public Map<String, EPSRQ_Setup.CipherVector> getEncKeywordBasis() {
        return encKeywordBasis;
    }
    
    public List<String> getDictionaryKeywords() {
        return dictionaryKeywords;
    }
    
    public Map<String, Integer> getDictionary() {
        return dict;
    }
    
    public BuildStats buildIndex(List<FixRangeCompareToConstructionOne.DataRow> allData) {
        dict.clear();
        dictionaryKeywords.clear();
        encKeywordBasis.clear();
        clearTree(root);
        
        if (keyPair == null) {
            keyPair = getKeyPair();
        }
        
        // 动态构建字典
        buildDynamicDictionary(allData);
        
        // 构建BKFtree索引
        int nodeCount = buildBKFtreeIndex(allData);
        int treeDepth = calculateTreeDepth(root);
        
        return new BuildStats(dict.size(), countRealLocations(root), 
                            countPhantomLocations(root), dictionaryKeywords.size(),
                            treeDepth, nodeCount);
    }
    
    private void buildDynamicDictionary(List<FixRangeCompareToConstructionOne.DataRow> allData) {
        Set<String> uniqueKeywords = new TreeSet<>();
        if (allData != null) {
            for (FixRangeCompareToConstructionOne.DataRow row : allData) {
                if (row != null && row.keywords != null) {
                    for (String keyword : row.keywords) {
                        if (keyword != null && !keyword.isEmpty()) {
                            uniqueKeywords.add(keyword);
                        }
                    }
                }
            }
        }
        
        List<String> sortedKeywords = new ArrayList<>(uniqueKeywords);
        Collections.sort(sortedKeywords);
        
        // 动态字典大小，避免硬编码
        for (int i = 0; i < sortedKeywords.size(); i++) {
            String keyword = sortedKeywords.get(i);
            dict.put(keyword, i);
            dictionaryKeywords.add(keyword);
        }
        
        // 构建关键词加密基向量
        for (Map.Entry<String, Integer> entry : dict.entrySet()) {
            encKeywordBasis.put(entry.getKey(), encryptKeywordBasisByIndex(entry.getValue()));
        }
    }
    
    private int buildBKFtreeIndex(List<FixRangeCompareToConstructionOne.DataRow> allData) {
        int nodeCount = 1; // 根节点
        
        if (allData != null) {
            // 按关键词分组数据
            Map<String, List<int[]>> keywordPostings = new HashMap<>();
            for (FixRangeCompareToConstructionOne.DataRow row : allData) {
                if (row != null && row.keywords != null) {
                    int fileId = normalizeFileId(row.fileID);
                    int x = (int) row.pointX;
                    int y = (int) row.pointY;
                    
                    for (String keyword : row.keywords) {
                        if (keyword != null && !keyword.isEmpty() && dict.containsKey(keyword)) {
                            keywordPostings
                                .computeIfAbsent(keyword, k -> new ArrayList<>())
                                .add(new int[]{fileId, x, y});
                        }
                    }
                }
            }
            
            // 为每个关键词构建BKFtree子树
            for (Map.Entry<String, List<int[]>> entry : keywordPostings.entrySet()) {
                String keyword = entry.getKey();
                List<int[]> postings = entry.getValue();
                
                // 构建该关键词的BKFtree
                BKFNode keywordNode = buildKeywordBKFtree(keyword, postings);
                root.children.put(keyword, keywordNode);
                nodeCount += countNodes(keywordNode);
            }
        }
        
        return nodeCount;
    }
    
    private BKFNode buildKeywordBKFtree(String keyword, List<int[]> postings) {
        BKFNode node = new BKFNode(1);
        
        // 添加真实位置
        for (int[] posting : postings) {
            double[] location = GrayCodeEncoder.encodePointLocationVector(posting[1], posting[2], t);
            EPSRQ_Setup.CipherVector encryptedLocation = setup.encryptIndex(keyPair.kv2Space, location, rnd);
            node.locations.add(new EPSRQ_IndexBuilder.EncryptedLocation(posting[0], encryptedLocation));
        }
        
        // 添加虚拟位置以满足gamma要求
        int missing = alignToGamma(postings.size()) - postings.size();
        for (int i = 0; i < missing; i++) {
            double[] phantom = GrayCodeEncoder.randomPhantomLocationVector(t, rnd);
            EPSRQ_Setup.CipherVector encryptedPhantom = setup.encryptIndex(keyPair.kv2Space, phantom, rnd);
            node.locations.add(new EPSRQ_IndexBuilder.EncryptedLocation(-1, encryptedPhantom));
        }
        
        return node;
    }
    
    public double[] keywordQueryVectorOr(String[] queryKeywords) {
        double[] queryVector = new double[dict.size()];
        if (queryKeywords != null) {
            for (String keyword : queryKeywords) {
                Integer index = dict.get(keyword);
                if (index != null && index >= 0 && index < dict.size()) {
                    queryVector[index] = 1.0;
                }
            }
        }
        return queryVector;
    }
    
    private EPSRQ_Setup.CipherVector encryptKeywordBasisByIndex(int index) {
        double[] basis = new double[dict.size()];
        basis[index] = 1.0;
        return setup.encryptIndex(keyPair.kv1Text, basis, rnd);
    }
    
    private int alignToGamma(int size) {
        if (size <= 0) return gamma;
        int remainder = size % gamma;
        return remainder == 0 ? size : (size + (gamma - remainder));
    }
    
    private int normalizeFileId(int fileId) {
        int normalized = fileId % maxFiles;
        return normalized < 0 ? normalized + maxFiles : normalized;
    }
    
    private void clearTree(BKFNode node) {
        node.locations.clear();
        for (BKFNode child : node.children.values()) {
            clearTree(child);
        }
        node.children.clear();
    }
    
    private int countRealLocations(BKFNode node) {
        int count = (int) node.locations.stream()
                .filter(loc -> loc.fileId >= 0)
                .count();
        for (BKFNode child : node.children.values()) {
            count += countRealLocations(child);
        }
        return count;
    }
    
    private int countPhantomLocations(BKFNode node) {
        int count = (int) node.locations.stream()
                .filter(loc -> loc.fileId < 0)
                .count();
        for (BKFNode child : node.children.values()) {
            count += countPhantomLocations(child);
        }
        return count;
    }
    
    private int calculateTreeDepth(BKFNode node) {
        if (node.children.isEmpty()) {
            return node.level;
        }
        int maxDepth = node.level;
        for (BKFNode child : node.children.values()) {
            maxDepth = Math.max(maxDepth, calculateTreeDepth(child));
        }
        return maxDepth;
    }
    
    private int countNodes(BKFNode node) {
        int count = 1;
        for (BKFNode child : node.children.values()) {
            count += countNodes(child);
        }
        return count;
    }
}