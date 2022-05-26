/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>
#include <algorithm>
#include <vector>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"

namespace badgerdb {

/**
 * @brief Datatype enumeration type.
 */
    enum Datatype {
        INTEGER = 0,
        DOUBLE = 1,
        STRING = 2
    };

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
    enum Operator {
        LT,    /* Less Than */
        LTE,    /* Less Than or Equal to */
        GTE,    /* Greater Than or Equal to */
        GT        /* Greater Than */
    };

/**
 * @brief Size of String key.
 */
    const int STRINGSIZE = 10;

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key               rid
    const int INTARRAYLEAFSIZE = (Page::SIZE - sizeof(PageId)) / (sizeof(int) + sizeof(RecordId));

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                     sibling ptr               key               rid
    const int DOUBLEARRAYLEAFSIZE = (Page::SIZE - sizeof(PageId)) / (sizeof(double) + sizeof(RecordId));

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                    sibling ptr           key                      rid
    const int STRINGARRAYLEAFSIZE = (Page::SIZE - sizeof(PageId)) / (10 * sizeof(char) + sizeof(RecordId));

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo                  key       pageNo
    const int INTARRAYNONLEAFSIZE = (Page::SIZE - sizeof(int) - sizeof(PageId)) / (sizeof(int) + sizeof(PageId));

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                        level        extra pageNo                 key            pageNo   -1 due to structure padding
    const int DOUBLEARRAYNONLEAFSIZE =
            ((Page::SIZE - sizeof(int) - sizeof(PageId)) / (sizeof(double) + sizeof(PageId))) - 1;

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                        level        extra pageNo             key                   pageNo
    const int STRINGARRAYNONLEAFSIZE =
            (Page::SIZE - sizeof(int) - sizeof(PageId)) / (10 * sizeof(char) + sizeof(PageId));

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to functions that 
 * add to or make changes to the leaf node pages of the tree. Is templated for the key member.
 */
    template<class T>
    class RIDKeyPair {
    public:
        RecordId rid;
        T key;

        void set(RecordId r, T k) {
            rid = r;
            key = k;
        }

        RIDKeyPair(RecordId r, T k) {
            set(r, k);
        }
    };

/**
 * @brief Structure to store a key page pair which is used to pass the key and page to functions that make 
 * any modifications to the non leaf pages of the tree.
*/
    template<class T>
    class PageKeyPair {
    public:
        PageId pageNo;
        T key;

        PageKeyPair():pageNo(), key() {}

        PageKeyPair(const PageKeyPair<T>& other) {
            set(other.pageNo, other.key);
        }

        PageKeyPair(int p, T k): pageNo(p), key(k) {}

        void set(int p, T k) {
            pageNo = p;
            key = k;
        }
    };

    template<class T>
    bool operator<(const PageKeyPair<T> &p1, const PageKeyPair<T> &p2) {
        return p1.key < p2.key;
    }


/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
    template<class T>
    bool operator<(const RIDKeyPair<T> &r1, const RIDKeyPair<T> &r2) {
        if (r1.key != r2.key)
            return r1.key < r2.key;
        else
            return r1.rid.page_number < r2.rid.page_number;
    }

/**
 * @brief The meta page, which holds metadata for Index file, is always first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
    struct IndexMetaInfo {
        /**
         * Name of base relation.
         */
        char relationName[20];

        /**
         * Offset of attribute, over which index is built, inside the record stored in pages.
         */
        int attrByteOffset;

        /**
         * Type of the attribute over which index is built.
         */
        Datatype attrType;

        /**
         * Page number of root page of the B+ Tree inside the file index file.
         */
        PageId rootPageNo;
    };

/*
Each node is a page, so once we read the page in we just cast the pointer to the page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in the pages for the index file depending on what kind of 
node they are. The level memeber of each non leaf structure seen below is set to 1 if the nodes 
at this level are just above the leaf nodes. Otherwise set to 0.
*/


/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
*/
    struct NonLeafNodeInt {
        /**
         * Level of the node in the tree.
         */
        int level;

        /**
         * Stores keys.
         */
        int keyArray[INTARRAYNONLEAFSIZE];

        /**
         * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
         */
        PageId pageNoArray[INTARRAYNONLEAFSIZE + 1];

        static int getKeyArraySize() {
            return INTARRAYNONLEAFSIZE;
        }

        static int getPageNoArraySize() {
            return INTARRAYNONLEAFSIZE + 1;
        }

        int getKey(int idx) {
            return keyArray[idx];
        }

        PageId getPageNo(int idx) {
            return pageNoArray[idx];
        }

        void setKey(int idx, int key) {
            keyArray[idx] = key;
        }

        void setPageNo(int idx, PageId PID) {
            pageNoArray[idx] = PID;
        }

        static int invalidKey() {
            return -1;
        }

        void clear() {
            for (int i = 0; i < getKeyArraySize(); i++) {
                keyArray[i] = invalidKey();
                pageNoArray[i] = Page::INVALID_NUMBER;
            }
            pageNoArray[getKeyArraySize() - 1] = Page::INVALID_NUMBER;
        }

        bool isFull() {
            return getKey(getKeyArraySize() - 1) != invalidKey();
        }
    };

/**
 * @brief Structure for all non-leaf nodes when the key is of DOUBLE type.
*/
    struct NonLeafNodeDouble {
        /**
         * Level of the node in the tree.
         */
        int level;

        /**
         * Stores keys.
         */
        double keyArray[DOUBLEARRAYNONLEAFSIZE];

        /**
         * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
         */
        PageId pageNoArray[DOUBLEARRAYNONLEAFSIZE + 1];

        static int getKeyArraySize() {
            return DOUBLEARRAYNONLEAFSIZE;
        }

        static int getPageNoArraySize() {
            return DOUBLEARRAYNONLEAFSIZE + 1;
        }

        double getKey(int idx) {
            return keyArray[idx];
        }

        PageId getPageNo(int idx) {
            return pageNoArray[idx];
        }

        void setKey(int idx, double key) {
            keyArray[idx] = key;
        }

        void setPageNo(int idx, PageId PID) {
            pageNoArray[idx] = PID;
        }

        static double invalidKey() {
            return -1;
        }

        void clear() {
            for (int i = 0; i < getKeyArraySize(); i++) {
                keyArray[i] = invalidKey();
                pageNoArray[i] = Page::INVALID_NUMBER;
            }
            pageNoArray[getKeyArraySize() - 1] = Page::INVALID_NUMBER;
        }

        bool isFull() {
            return getKey(getKeyArraySize() - 1) != invalidKey();
        }
    };

/**
 * @brief Structure for all non-leaf nodes when the key is of STRING type.
*/

    struct NonLeafNodeString {
        /**
         * Level of the node in the tree.
         */
        int level;

        /**
         * Stores keys.
         */
        char keyArray[STRINGARRAYNONLEAFSIZE][STRINGSIZE];

        /**
         * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
         */
        PageId pageNoArray[STRINGARRAYNONLEAFSIZE + 1];

        static int getKeyArraySize() {
            return STRINGARRAYNONLEAFSIZE;
        }

        static int getPageNoArraySize() {
            return STRINGARRAYNONLEAFSIZE + 1;
        }

        std::string getKey(int idx) {
            return std::string(keyArray[idx], STRINGSIZE);
        }

        PageId getPageNo(int idx) {
            return pageNoArray[idx];
        }

        void setKey(int idx, std::string key) {
            strncpy(keyArray[idx], invalidKey().c_str(), STRINGSIZE);
            for (int i = 0; i < STRINGSIZE && i < key.length(); i++) {
                keyArray[idx][i] = key[i];
            }
        }

        void setPageNo(int idx, PageId PID) {
            pageNoArray[idx] = PID;
        }

        static std::string invalidKey() {
            std::string k;
            for (int i = 0; i < STRINGSIZE; i++) {
                k += '\0';
            }
            return k;
        }

        void clear() {
            for (int i = 0; i < getKeyArraySize(); i++) {
                strncpy(keyArray[i], invalidKey().c_str(), STRINGSIZE);
                pageNoArray[i] = Page::INVALID_NUMBER;
            }
            pageNoArray[getKeyArraySize() - 1] = Page::INVALID_NUMBER;
        }

        bool isFull() {
            return getKey(getKeyArraySize() - 1) != invalidKey();
        }
    };

/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
*/
    struct LeafNodeInt {
        /**
         * Stores keys.
         */
        int keyArray[INTARRAYLEAFSIZE];

        /**
         * Stores RecordIds.
         */
        RecordId ridArray[INTARRAYLEAFSIZE];

        /**
         * Page number of the leaf on the right side.
           * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
         */
        PageId rightSibPageNo;

        static int getKeyArraySize() {
            return INTARRAYLEAFSIZE;
        }

        static int getRIDArraySize() {
            return INTARRAYLEAFSIZE;
        }

        int getKey(int idx) {
            return keyArray[idx];
        }

        RecordId getRID(int idx) {
            return ridArray[idx];
        }

        void setKey(int idx, int key) {
            keyArray[idx] = key;
        }

        void setRID(int idx, RecordId RID) {
            ridArray[idx] = RID;
        }

        static int invalidKey() {
            return -1;
        }

        void clear() {
            for (int i = 0; i < getKeyArraySize(); i++) {
                keyArray[i] = invalidKey();
            }
        }

        bool isFull() {
            return getKey(getKeyArraySize() - 1) != invalidKey();
        }

    };

/**
 * @brief Structure for all leaf nodes when the key is of DOUBLE type.
*/
    struct LeafNodeDouble {
        /**
         * Stores keys.
         */
        double keyArray[DOUBLEARRAYLEAFSIZE];

        /**
         * Stores RecordIds.
         */
        RecordId ridArray[DOUBLEARRAYLEAFSIZE];

        /**
         * Page number of the leaf on the right side.
           * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
         */
        PageId rightSibPageNo;
//        bool hasRightSib;

        static int getKeyArraySize() {
            return DOUBLEARRAYLEAFSIZE;
        }

        static int getRIDArraySize() {
            return DOUBLEARRAYLEAFSIZE;
        }

        double getKey(int idx) {
            return keyArray[idx];
        }

        RecordId getRID(int idx) {
            return ridArray[idx];
        }

        void setKey(int idx, double key) {
            keyArray[idx] = key;
        }

        void setRID(int idx, RecordId RID) {
            ridArray[idx] = RID;
        }

        static double invalidKey() {
            return -1;
        }

        void clear() {
            for (int i = 0; i < getKeyArraySize(); i++) {
                keyArray[i] = invalidKey();
            }
        }

        bool isFull() {
            return getKey(getKeyArraySize() - 1) != invalidKey();
        }
    };

/**
 * @brief Structure for all leaf nodes when the key is of STRING type.
*/
    struct LeafNodeString {
        /**
         * Stores keys.
         */
        char keyArray[STRINGARRAYLEAFSIZE][STRINGSIZE];

        /**
         * Stores RecordIds.
         */
        RecordId ridArray[STRINGARRAYLEAFSIZE];

        /**
         * Page number of the leaf on the right side.
           * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
         */
        PageId rightSibPageNo;
//        bool hasRightSib;

        static int getKeyArraySize() {
            return STRINGARRAYLEAFSIZE;
        }

        static int getRIDArraySize() {
            return STRINGARRAYLEAFSIZE;
        }

        std::string getKey(int idx) {
            return std::string(keyArray[idx], STRINGSIZE);
        }

        RecordId getRID(int idx) {
            return ridArray[idx];
        }

        void setKey(int idx, std::string key) {
            strncpy(keyArray[idx], invalidKey().c_str(), STRINGSIZE);
            strncpy(keyArray[idx], key.c_str(), STRINGSIZE);
        }

        void setRID(int idx, RecordId RID) {
            ridArray[idx] = RID;
        }

        static std::string invalidKey() {
            std::string k;
            for (int i = 0; i < STRINGSIZE; i++) {
                k += '\0';
            }
            return k;
        }

        void clear() {
            for (int i = 0; i < getKeyArraySize(); i++) {
                strncpy(keyArray[i], invalidKey().c_str(), STRINGSIZE);
            }
        }

        bool isFull() {
            std::string test = getKey(getKeyArraySize() - 1);
            return getKey(getKeyArraySize() - 1) != invalidKey();
        }


    };

//    int validateKeys(Page page);

    /**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute of a
 * relation. This index supports only one scan at a time.
*/
    class BTreeIndex {

    private:
        template<class NT, typename T>
        friend
        class NonLeafNodeContainer;

        template<class NT, typename T>
        friend
        class LeafNodeContainer;

        /**
         * File object for the index file.
         */
        File *file;

        /**
         * Buffer Manager Instance.
         */
        BufMgr *bufMgr;

        /**
         * Page number of meta page.
         */
        PageId headerPageNum;

        /**
         * page number of root page of B+ tree inside index file.
         */
        PageId rootPageNum;

        /**
         * Datatype of attribute over which index is built.
         */
        Datatype attributeType;

        /**
         * Offset of attribute, over which index is built, inside records.
         */
        int attrByteOffset;

        /**
         * Number of keys in leaf node, depending upon the type of key.
         */
        int leafOccupancy;

        /**
         * Number of keys in non-leaf node, depending upon the type of key.
         */
        int nodeOccupancy;


        // MEMBERS SPECIFIC TO SCANNING

        /**
         * True if an index scan has been started.
         */
        bool scanExecuting;

        /**
         * Index of next entry to be scanned in current leaf being scanned.
         */
        int nextEntry;

        /**
         * Page number of current page being scanned.
         */
        PageId currentPageNum;

        /**
         * Current Page being scanned.
         */
        Page *currentPageData;

        /**
         * Low INTEGER value for scan.
         */
        int lowValInt;

        /**
         * Low DOUBLE value for scan.
         */
        double lowValDouble;

        /**
         * Low STRING value for scan.
         */
        std::string lowValString;

        /**
         * High INTEGER value for scan.
         */
        int highValInt;

        /**
         * High DOUBLE value for scan.
         */
        double highValDouble;

        /**
         * High STRING value for scan.
         */
        std::string highValString;

        /**
         * Low Operator. Can only be GT(>) or GTE(>=).
         */
        Operator lowOp;

        /**
         * High Operator. Can only be LT(<) or LTE(<=).
         */
        Operator highOp;


    public:
        IndexMetaInfo meta;

        /**
         * BTreeIndex Constructor.
           * Check to see if the corresponding index file exists. If so, open the file.
           * If not, create it and insert entries for every tuple in the base relation using FileScan class.
         *
         * @param relationName        Name of file.
         * @param outIndexName        Return the name of index file.
         * @param bufMgrIn						Buffer Manager Instance
         * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
         * @param attrType						Datatype of attribute over which index is built
         * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
         */
        BTreeIndex(const std::string &relationName, std::string &outIndexName,
                   BufMgr *bufMgrIn, const int attrByteOffset, const Datatype attrType);


        /**
         * BTreeIndex Destructor.
           * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
           * and delete file instance thereby closing the index file.
           * Destructor should not throw any exceptions. All exceptions should be caught in here itself.
           * */
        ~BTreeIndex();


        /**
           * Insert a new entry using the pair <value,rid>.
           * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
           * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
           * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
           * Make sure to unpin pages as soon as you can.
         * @param key			Key to insert, pointer to integer/double/char string
         * @param rid			Record ID of a record whose entry is getting inserted into the index.
          **/
        const void insertEntry(const void *key, const RecordId rid);


        /**
           * Begin a filtered scan of the index.  For instance, if the method is called
           * using ("a",GT,"d",LTE) then we should seek all entries with a value
           * greater than "a" and less than or equal to "d".
           * If another scan is already executing, that needs to be ended here.
           * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
           * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
         * @param lowVal	Low value of range, pointer to integer / double / char string
         * @param lowOp		Low operator (GT/GTE)
         * @param highVal	High value of range, pointer to integer / double / char string
         * @param highOp	High operator (LT/LTE)
         * @throws  BadOpcodesException If lowOp and highOp do not contain
         * one of their their expected values
         * @throws  BadScanrangeException If lowVal > highval
           * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
          **/
        const void startScan(const void *lowVal, const Operator lowOp, const void *highVal, const Operator highOp);


        /**
           * Fetch the record id of the next index entry that matches the scan.
           * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
         * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
           * @throws ScanNotInitializedException If no scan has been initialized.
           * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
          **/
        const void scanNext(RecordId &outRid);  // returned record id


        /**
           * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
           * @throws ScanNotInitializedException If no scan has been initialized.
          **/
        const void endScan();

        PageId findLeaf(const void *key, std::vector<PageId>& vec);

        PageId findLeaf(const void *key);

        bool insertLeaf(const void *key, const RecordId rid, PageId nodePID, void* pk);

        bool insertNonLeaf(PageId nodePID, void *pk, bool isAboveLeaf);

        void newRoot(void *pk, PageId leftChildPID, bool isAboveLeaf);
    };

    // Generic NonLeafNode
    template<class NT, typename T>
    struct NonLeafNodeContainer {
        Datatype attrType;
        Page* page;
        PageId PID;
        BTreeIndex* index;
        NT node;

        NonLeafNodeContainer(const NonLeafNodeContainer<NT,T>& other) {
            attrType = other.attrType;
            page= other.page;
            PID= other.PID;
            index= other.index;
            node= other.node;
        }

        NonLeafNodeContainer(BTreeIndex *index, Page &page, PageId PID, bool newNode=false) : index(index), page(&page), PID(PID) {
            attrType = index->meta.attrType;
            if (newNode) {
                node.clear();
                write();
            } else {
                read();
            }
        }

        void read() {
            node = *(NT *) page;
        }

        const void write() {
            *page = *(Page *) &node;
            index->file->writePage(PID, *page);
        }

        PageId search(T key) {
            int idx = 0;
            while (node.getKey(idx) < key && node.getKey(idx) != NT::invalidKey() && idx < NT::getKeyArraySize()) {
                idx++;
            }
            return node.getPageNo(idx);
        }

        void insert(PageKeyPair<T> pair) {
            std::vector<PageKeyPair<T>> pairs;
            PageId firstPID = node.getPageNo(0);

            pairs.push_back(pair);
            for (int i = 0; node.getKey(i) != NT::invalidKey() && i < NT::getKeyArraySize(); i++) {
                pairs.push_back(PageKeyPair<T>(node.getPageNo(i + 1), node.getKey(i)));
            }
            sort(pairs.begin(), pairs.end());

            node.clear();
            node.setPageNo(0, firstPID);
            int keyIdx = 0;
            for (auto it = pairs.begin(); it != pairs.end(); it++, keyIdx++) {
                node.setKey(keyIdx, it->key);
                node.setPageNo(keyIdx + 1, it->pageNo);
            }

            write();
        }

        PageKeyPair<T> split(PageKeyPair<T> pair) {
            std::vector<PageKeyPair<T>> pairs;
            int keyIdx = 0;
            int length = NT::getKeyArraySize();
            T midKey;
            Page rightPage;
            PageId rightPID;

            rightPage = index->file->allocatePage(rightPID);

            NonLeafNodeContainer<NT, T> rightNodeContainer(index, rightPage, rightPID, true);

            PageId firstPID = node.getPageNo(0);

            // Insert and Sort
            pairs.push_back(pair);
            for (int i = 0; i < length; i++) {
                pairs.push_back(PageKeyPair<T>(node.getPageNo(i+1), node.getKey(i)));
            }
            node.clear();
            sort(pairs.begin(), pairs.end());

            // Redistribute Left Node
            node.setPageNo(0, firstPID);
            for (; keyIdx < length / 2; keyIdx++) {
                node.setKey(keyIdx, pairs[keyIdx].key);
                node.setPageNo(keyIdx + 1, pairs[keyIdx].pageNo);
            }

            // Pull up Middle entry
            midKey = node.getKey(keyIdx);
            rightNodeContainer.node.setPageNo(0, pairs[keyIdx].pageNo);
            keyIdx++;

            // Redistribute Right Node
            for (int nodeKeyIdx = 0; keyIdx <= length; keyIdx++, nodeKeyIdx++) {
                rightNodeContainer.node.setKey(nodeKeyIdx, pairs[keyIdx].key);
                rightNodeContainer.node.setPageNo(nodeKeyIdx + 1, pairs[keyIdx].pageNo);
            }

            write();
            rightNodeContainer.node.level = node.level;
            rightNodeContainer.write();

            PageKeyPair<T> out(rightNodeContainer.PID, midKey);
            return out;
        }
    };

    // Generic LeafNode
    template<class NT, typename T>
    struct LeafNodeContainer {
        Datatype attrType;
        Page* page;
        PageId PID;
        BTreeIndex* index;
        NT node;

        LeafNodeContainer(const LeafNodeContainer<NT,T>& other) {
            attrType = other.attrType;
            page= other.page;
            PID= other.PID;
            index= &other.index;
            node= other.node;
        }

        LeafNodeContainer(BTreeIndex* index, Page &page, PageId PID, bool newNode=false) : index(index), page(&page), PID(PID) {
            attrType = index->meta.attrType;
            if (newNode) {
                node.rightSibPageNo = 0;
                node.rightSibPageNo = false;
                node.clear();
                write();
            } else {
                read();
            }
        }

        void read() {
            node = *(NT *) page;
        }

        const void write() {
            *page = *(Page *) &node;
            index->file->writePage(PID, *page);
        }

        RecordId search(T key) {
            int idx = 0;
            while (node.getKey(idx) < key && node.getKey(idx) != NT::invalidKey() && idx < NT::getKeyArraySize()) {
                idx++;
            }

            return node.getRID(idx);
        }

        RecordId search(T key, int& entryIdx) {
            int idx = 0;
            while (node.getKey(idx) < key && node.getKey(idx) != NT::invalidKey() && idx < NT::getKeyArraySize()) {
                idx++;
            }
            entryIdx = idx;
            return node.getRID(idx);
        }

        void insert(RIDKeyPair<T> pair) {
            std::vector<RIDKeyPair<T>> pairs;
            int keyIdx = 0;

            // Sort Entries
            pairs.push_back(pair);
            for (int i = 0; node.getKey(i) != NT::invalidKey() && i < NT::getKeyArraySize(); i++) {
                pairs.push_back(RIDKeyPair<T>(node.getRID(i), node.getKey(i)));
            }
            sort(pairs.begin(), pairs.end());

            // Insert Entries
            node.clear();
            for (auto it = pairs.begin(); it != pairs.end(); it++) {
                node.setKey(keyIdx, it->key);
                node.setRID(keyIdx, it->rid);
                keyIdx++;
            }

            write();
        }

        PageKeyPair<T> split(RIDKeyPair<T> pair) {
            std::vector<RIDKeyPair<T>> pairs;
            int keyIdx = 0;
            T midKey;
            Page rightPage;
            PageId rightPID;

            rightPage = index->file->allocatePage(rightPID);

            LeafNodeContainer<NT, T> rightNodeContainer(index, rightPage, rightPID, true);

            // Sort
            pairs.push_back(pair);
            for (int i = 0; node.getKey(i) != NT::invalidKey() && i < NT::getKeyArraySize(); i++) {
                pairs.push_back(RIDKeyPair<T>(node.getRID(i), node.getKey(i)));
            }
            sort(pairs.begin(), pairs.end());

            // Redistribute Left Node
            node.clear();
            for (; keyIdx < NT::getKeyArraySize() / 2; keyIdx++) {
                node.setKey(keyIdx, pairs[keyIdx].key);
                node.setRID(keyIdx, pairs[keyIdx].rid);
            }

            // Copy up Middle entry
            midKey = pairs[keyIdx].key;

            // Redistribute Right Node
            for (int nodeKeyIdx = 0; keyIdx <= NT::getKeyArraySize() ; keyIdx++, nodeKeyIdx++) {
                rightNodeContainer.node.setKey(nodeKeyIdx, pairs[keyIdx].key);
                rightNodeContainer.node.setRID(nodeKeyIdx, pairs[keyIdx].rid);
            }

//            if (node.hasRightSib) {
//                rightNodeContainer.node.rightSibPageNo = node.rightSibPageNo;
//            }

            rightNodeContainer.node.rightSibPageNo = node.rightSibPageNo;
            rightNodeContainer.write();

            node.rightSibPageNo = rightNodeContainer.PID;
            write();

            PageKeyPair<T> out(rightNodeContainer.PID, midKey);
            return out;
        }



    };

    std::string padStr(std::string str);

}
