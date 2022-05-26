/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

    BTreeIndex::BTreeIndex(const std::string &relationName,
                           std::string &outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType) {
        Page metaPage, rootPage;

        this->bufMgr = bufMgrIn;
        this->attrByteOffset = attrByteOffset;
        this->attributeType = attrType;

        // Open or Create index file
        std::ostringstream idxStr;
        std::string indexName;

        idxStr << relationName << "." << attrByteOffset;
        outIndexName = idxStr.str();

        // Retrieve index file
        try { // Open
            std::string buffer;
            this->file = new BlobFile(outIndexName, false);
            printf("Using existing Index\n");
            metaPage = file->readPage(1);
            meta = *(IndexMetaInfo*) &metaPage;
        } catch (FileNotFoundException e) { // Create
            printf("Create New Index\n");
            PageId metaPID;

            this->file = new BlobFile(outIndexName, true);

            // Initialize and allocate IndexMetaInfo
            strcpy(this->meta.relationName, relationName.c_str());
            this->meta.attrByteOffset = attrByteOffset;
            this->meta.attrType = attrType;
            metaPage = file->allocatePage(metaPID);

            // Write root node to disk and assign rootPageNo
            rootPage = file->allocatePage(meta.rootPageNo);
            switch (attrType) {
                case INTEGER: {
                    LeafNodeContainer<LeafNodeInt, int> rootNode(this, rootPage, meta.rootPageNo, true);
                    this->meta.rootPageNo = rootNode.PID;
                    break;
                }
                case DOUBLE: {
                    LeafNodeContainer<LeafNodeDouble, double> rootNode(this, rootPage, meta.rootPageNo, true);
                    this->meta.rootPageNo = rootNode.PID;
                    break;
                }
                default: {
                    LeafNodeContainer<LeafNodeString, std::string> rootNode(this, rootPage, meta.rootPageNo, true);
                    this->meta.rootPageNo = rootNode.PID;
                }
            }

            // Write meta to Disk
            metaPage = *(Page *) &meta;
            file->writePage(1, metaPage);

            // Index relation
            printf("Indexing Relation...\n");
            FileScan fscan(relationName, bufMgr);
            int i =0;
            try {
                RecordId scanRid;
                while (1) {
                    fscan.scanNext(scanRid);
                    std::string recordStr = fscan.getRecord();
                    const char *record = recordStr.c_str();
                    switch (attrType) {
                        case INTEGER: {
                            int key = *((int *) (record + meta.attrByteOffset));
                            this->insertEntry(&key, scanRid);
                            break;
                        }
                        case DOUBLE: {
                            double key = *((double *) (record + meta.attrByteOffset));
                            this->insertEntry(&key, scanRid);
                            break;
                        }
                        default: {
                            std::string key = ((char *) (record + meta.attrByteOffset));
                            this->insertEntry(&key, scanRid);
                            i++;
                            break;
                        }
                    }
                }
            }
            catch (EndOfFileException e) {
                std::cout << "Read all records" << std::endl;
            }
        }
    }
// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

    BTreeIndex::~BTreeIndex() {
        file->~File();
    }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

    PageId BTreeIndex::findLeaf(const void *key, std::vector<PageId> &vec) {
        Page curNodePage;
        PageId curNodePID;
        bool isLeaf = false;

        curNodePID = meta.rootPageNo;
        if (curNodePID == 2) {
            return curNodePID;
        }

        while (!isLeaf) {
            vec.push_back(curNodePID);
            curNodePage = file->readPage(curNodePID);
            switch (attributeType) {
                case INTEGER: {
                    NonLeafNodeContainer<NonLeafNodeInt, int> container(this, curNodePage, curNodePID);
                    if (container.node.level == 1) {
                        isLeaf = true;
                    }
                    curNodePID = container.search(*(int *) key);
                    break;
                }
                case DOUBLE: {
                    NonLeafNodeContainer<NonLeafNodeDouble, double> container(this, curNodePage, curNodePID);
                    if (container.node.level == 1) {
                        isLeaf = true;
                    }
                    curNodePID = container.search(*(double *) key);
                    break;
                }
                default: {
                    NonLeafNodeContainer<NonLeafNodeString, std::string> container(this, curNodePage, curNodePID);
                    if (container.node.level == 1) {
                        isLeaf = true;
                    }
                    curNodePID = container.search(*(std::string*)key);
                    break;
                }
            }
        }

        return curNodePID;
    }

    PageId BTreeIndex::findLeaf(const void *key) {
        Page curNodePage;
        PageId curNodePID;
        bool isLeaf = false;

        curNodePID = meta.rootPageNo;
        if (curNodePID == 2) {
            return curNodePID;
        }

        while (!isLeaf) {
            curNodePage = file->readPage(curNodePID);
            switch (attributeType) {
                case INTEGER: {
                    NonLeafNodeContainer<NonLeafNodeInt, int> container(this, curNodePage, curNodePID);
                    if (container.node.level == 1) {
                        isLeaf = true;
                    }
                    curNodePID = container.search(*(int *) key);
                    break;
                }
                case DOUBLE: {
                    NonLeafNodeContainer<NonLeafNodeDouble, double> container(this, curNodePage, curNodePID);
                    if (container.node.level == 1) {
                        isLeaf = true;
                    }
                    curNodePID = container.search(*(double *) key);
                    break;
                }
                default: {
                    NonLeafNodeContainer<NonLeafNodeString, std::string> container(this, curNodePage, curNodePID);
                    if (container.node.level == 1) {
                        isLeaf = true;
                    }
                    curNodePID = container.search(std::string((char *) key, STRINGSIZE));
                    break;
                }
            }
        }

        return curNodePID;
    }

    bool BTreeIndex::insertLeaf(const void *key, const RecordId rid, PageId nodePID, void *pk) {
        bool propagateSplit = false;
        Page page = file->readPage(nodePID);

        switch (attributeType) {
            case INTEGER: {
                LeafNodeContainer<LeafNodeInt, int> container(this, page, nodePID);
                if (container.node.isFull()) {
                    propagateSplit = true;
                    *(PageKeyPair<int> *) pk = (container.split(RIDKeyPair<int>(rid, *(int *) key)));
                } else {
                    container.insert(RIDKeyPair<int>(rid, *(int *) key));
                }
                break;
            }
            case DOUBLE: {
                LeafNodeContainer<LeafNodeDouble, double> container(this, page, nodePID);
                if (container.node.isFull()) {
                    propagateSplit = true;
                    *(PageKeyPair<double> *) pk = container.split(RIDKeyPair<double>(rid, *(double *) key));
                } else {
                    container.insert(RIDKeyPair<double>(rid, *(double *) key));
                }
                break;
            }
            default: {
                LeafNodeContainer<LeafNodeString, std::string> container(this, page, nodePID);
                RIDKeyPair<std::string> newRK(rid, *(std::string*) key);
                if (container.node.isFull()) {
                    propagateSplit = true;
                    *(PageKeyPair<std::string> *) pk = container.split(newRK);
                } else {
                    container.insert(newRK);
                }
                break;
            }
        }
        return propagateSplit;
    }

    bool BTreeIndex::insertNonLeaf(PageId nodePID, void *pk, bool isAboveLeaf) {
        bool propagateSplit = false;
        Page page = file->readPage(nodePID);

        switch (attributeType) {
            case INTEGER: {
                NonLeafNodeContainer<NonLeafNodeInt, int> container(this, page, nodePID);
                container.node.level = isAboveLeaf ? 1 : 0;
                if (container.node.isFull()) {
                    *(PageKeyPair<int> *) pk = container.split(*(PageKeyPair<int> *) pk);
                } else {
                    container.insert(*(PageKeyPair<int> *) pk);
                    propagateSplit = false;
                }
                break;
            }
            case DOUBLE: {
                NonLeafNodeContainer<NonLeafNodeDouble, double> container(this, page, nodePID);
                container.node.level = isAboveLeaf ? 1 : 0;
                if (container.node.isFull()) {
                    *(PageKeyPair<double> *) pk = container.split(*(PageKeyPair<double> *) pk);
                } else {
                    container.insert(*(PageKeyPair<double> *) pk);
                    propagateSplit = false;
                }
                break;
            }
            default: {
                NonLeafNodeContainer<NonLeafNodeString, std::string> container(this, page, nodePID);
                container.node.level = isAboveLeaf ? 1 : 0;

                if (container.node.isFull()) {
                    *(PageKeyPair<std::string> *) pk = container.split(*(PageKeyPair<std::string> *) pk);
                } else {
                    container.insert(*(PageKeyPair<std::string> *) pk);
                    propagateSplit = false;
                }
            }
        }
        return propagateSplit;
    }

    void BTreeIndex::newRoot(void *pk, PageId leftChildPID, bool isAboveLeaf) {
        Page page;
        PageId PID;
        printf("new Root\n");
        page = file->allocatePage(PID);
        switch (attributeType) {
            case INTEGER: {
                NonLeafNodeContainer<NonLeafNodeInt, int> newRootContainer(this, page, PID, true);
                newRootContainer.node.level = isAboveLeaf ? 1 : 0;
                newRootContainer.node.setPageNo(0, leftChildPID);
                newRootContainer.insert(*(PageKeyPair<int> *) pk);
                meta.rootPageNo = newRootContainer.PID;
                break;
            }
            case DOUBLE: {
                NonLeafNodeContainer<NonLeafNodeDouble, double> newRootContainer(this, page, PID, true);
                newRootContainer.node.level = isAboveLeaf ? 1 : 0;
                newRootContainer.node.setPageNo(0, leftChildPID);
                newRootContainer.insert(*(PageKeyPair<double> *) pk);
                meta.rootPageNo = newRootContainer.PID;
                break;
            }
            default: {
                NonLeafNodeContainer<NonLeafNodeString, std::string> newRootContainer(this, page, PID, true);
                newRootContainer.node.level = isAboveLeaf ? 1 : 0;
                newRootContainer.node.setPageNo(0, leftChildPID);
                newRootContainer.insert(*(PageKeyPair<std::string> *) pk);
                meta.rootPageNo = newRootContainer.PID;
            }
        }
        Page metaPage = *(Page *) &meta;
        file->writePage(1, metaPage);
    }

    int splits = 1;

    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        Page curNodePage;
        PageId curNodePID;
        std::vector<PageId> nonLeafNodePIDs;
        bool isAboveLeaf;
        bool propagateSplit;

        PageKeyPair<int> curPKInt;
        PageKeyPair<double> curPKDouble;
        PageKeyPair<std::string> curPKString;

        // Find Target Leaf Node
        curNodePID = findLeaf(key, nonLeafNodePIDs);

        // Insert/Split Leaf Node
        switch (attributeType) {
            case INTEGER:
                propagateSplit = insertLeaf(key, rid, curNodePID, &curPKInt);
                break;
            case DOUBLE:
                propagateSplit = insertLeaf(key, rid, curNodePID, &curPKDouble);
                break;
            default:
                propagateSplit = insertLeaf(key, rid, curNodePID, &curPKString);
        }
        isAboveLeaf = true;

        // Propagate Middle Key up until Root Node
        while (propagateSplit && nonLeafNodePIDs.size() > 1) {
            curNodePID = nonLeafNodePIDs.back();
            nonLeafNodePIDs.pop_back();

            switch (attributeType) {
                case INTEGER:
                    propagateSplit = insertNonLeaf(curNodePID, &curPKInt, isAboveLeaf);
                    break;
                case DOUBLE:
                    propagateSplit = insertNonLeaf(curNodePID, &curPKDouble, isAboveLeaf);
                    break;
                default:
                    propagateSplit = insertNonLeaf(curNodePID, &curPKString, isAboveLeaf);
            }

            isAboveLeaf = isAboveLeaf ? false : isAboveLeaf;
        }

        // Propagate Middle Key to Root Node
        if (propagateSplit) {
            if (nonLeafNodePIDs.empty()) { // Leaf Node is Root => create root = NonLeafNode
                switch (attributeType) {
                    case INTEGER:
                        newRoot(&curPKInt, curNodePID, isAboveLeaf);
                        break;
                    case DOUBLE:
                        newRoot(&curPKDouble, curNodePID, isAboveLeaf);
                        break;
                    default:
                        newRoot(&curPKString, curNodePID, isAboveLeaf);
                }

            } else { // Insert/Split Root Node
                curNodePID = nonLeafNodePIDs.back();
                nonLeafNodePIDs.pop_back();
                switch(attributeType) {
                    case INTEGER:
                        propagateSplit = insertNonLeaf(curNodePID, &curPKInt, isAboveLeaf);
                        break;
                    case DOUBLE:
                        propagateSplit = insertNonLeaf(curNodePID, &curPKDouble, isAboveLeaf);
                        break;
                    default:
                        propagateSplit = insertNonLeaf(curNodePID, &curPKString, isAboveLeaf);
                }

                if (propagateSplit) {
                    switch (attributeType) {
                        case INTEGER:
                            newRoot(&curPKInt, curNodePID, false);
                            break;
                        case DOUBLE:
                            newRoot(&curPKDouble, curNodePID, false);
                            break;
                        default:
                            newRoot(&curPKString, curNodePID, false);
                    }
                }
            }
        }
    }

    std::string padStr(std::string str) {
        std::string newStr = "";
        int i = 0;
        for (; i < str.length() && i < STRINGSIZE; i++) {
            newStr += str[i];
        }

        for (; i < STRINGSIZE; i++) {
            newStr += '\0';
        }
        return newStr;
    }

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
    const void BTreeIndex::startScan(const void *lowValParm,
                                     const Operator lowOpParm,
                                     const void *highValParm,
                                     const Operator highOpParm) {
        Page leafPage;

        // Initialize Scan Parameters
        if (lowOpParm == LT || lowOpParm == LTE || highOpParm == GT || highOpParm == GTE) {
            throw BadOpcodesException();
        }
        lowOp = lowOpParm;
        highOp = highOpParm;
        switch (attributeType) {
            case INTEGER: {
                lowValInt = *(int *) lowValParm;
                highValInt = *(int *) highValParm;
                if (lowValInt > highValInt) {
                    throw BadScanrangeException();
                }
                break;
            }
            case DOUBLE: {
                lowValDouble = *(double *) lowValParm;
                highValDouble = *(double *) highValParm;
                if (lowValDouble > highValDouble) {
                    throw BadScanrangeException();
                }
                break;
            }
            default: {
                lowValString = padStr(std::string((char *) lowValParm, STRINGSIZE));
                highValString = padStr(std::string((char *) highValParm, STRINGSIZE));
                if (lowValString > highValString) {
                    throw BadScanrangeException();
                }
            }
        }
        scanExecuting = true;

        // Find Target Leaf
        currentPageNum = findLeaf(lowValParm);
        leafPage = file->readPage(currentPageNum);
        // Seek lowVal Entry
        switch (attributeType) {
            RecordId dummy;
            case INTEGER: {
                LeafNodeContainer<LeafNodeInt, int> container(this, leafPage, currentPageNum);
                container.search(lowValInt, nextEntry);
                if (lowOp == GT && lowValInt == container.node.getKey(nextEntry) || container.node.getKey(nextEntry) == LeafNodeInt::invalidKey()) {
                    scanNext(dummy);
                }
                break;
            }
            case DOUBLE: {
                LeafNodeContainer<LeafNodeDouble, double> container(this, leafPage, currentPageNum);
                container.search(lowValDouble, nextEntry);

                if (lowOp == GT && lowValDouble == container.node.getKey(nextEntry) || container.node.getKey(nextEntry) == LeafNodeDouble::invalidKey()) {
                    scanNext(dummy);
                }
                break;
            }
            default: {
                LeafNodeContainer<LeafNodeString, std::string> container(this, leafPage, currentPageNum);
                container.search(lowValString, nextEntry);
                if (lowOp == GT && lowValString == container.node.getKey(nextEntry) || container.node.getKey(nextEntry) == LeafNodeString::invalidKey()) {
                    scanNext(dummy);
                }
            }
        }
    }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

    const void BTreeIndex::scanNext(RecordId &outRid) {
        if (!scanExecuting) {
            throw ScanNotInitializedException();
        }
        Page curPage = file->readPage(currentPageNum);
        switch (attributeType) {
            case INTEGER: {
                LeafNodeContainer<LeafNodeInt, int> container(this, curPage, currentPageNum);
                if (nextEntry >= LeafNodeInt::getKeyArraySize() || container.node.getKey(nextEntry) == LeafNodeInt::invalidKey()) {
                    if (container.node.rightSibPageNo == 0) {
                        goto completed;
                    } else {
                        // Unpin prev page
                        nextEntry = 0;
                        currentPageNum = container.node.rightSibPageNo;
                        curPage = file->readPage(currentPageNum);
                    }
                }
                container = LeafNodeContainer<LeafNodeInt, int>(this, curPage, currentPageNum);
                if ((highOp == LT && container.node.getKey(nextEntry) >= highValInt) or
                    (highOp == LTE && container.node.getKey(nextEntry) > highValInt)) {
                    goto completed;
                }
                outRid = container.node.getRID(nextEntry);
                nextEntry++;
                break;
            }
            case DOUBLE: {
                LeafNodeContainer<LeafNodeDouble, double> container(this, curPage, currentPageNum);
                if (nextEntry >= LeafNodeDouble::getKeyArraySize() || container.node.getKey(nextEntry) == LeafNodeDouble::invalidKey()) {
                    if (container.node.rightSibPageNo == 0) {
                        goto completed;
                    } else {
                        // Unpin prev page
                        nextEntry = 0;
                        currentPageNum = container.node.rightSibPageNo;
                        curPage = file->readPage(currentPageNum);
                    }
                }
                container = LeafNodeContainer<LeafNodeDouble, double>(this, curPage, currentPageNum);
                if ((highOp == LT && container.node.getKey(nextEntry) >= highValDouble) or
                    (highOp == LTE && container.node.getKey(nextEntry) > highValDouble)) {
                    goto completed;
                }
                outRid = container.node.getRID(nextEntry);
//                printf("cur Key %f\n", container.node.getKey(nextEntry));
                nextEntry++;
                break;
            }
            default: {
                LeafNodeContainer<LeafNodeString, std::string> container(this, curPage, currentPageNum);
                if (nextEntry >= LeafNodeString::getKeyArraySize() || container.node.getKey(nextEntry) == LeafNodeString::invalidKey()) {
                    if (container.node.rightSibPageNo == 0) {
                        goto completed;
                    } else {
                        // Unpin prev page
                        nextEntry = 0;
                        currentPageNum = container.node.rightSibPageNo;
                        curPage = file->readPage(currentPageNum);
                    }
                }
                container = LeafNodeContainer<LeafNodeString, std::string>(this, curPage, currentPageNum);
                if ((highOp == LT && container.node.getKey(nextEntry) >= highValString) or
                    (highOp == LTE && container.node.getKey(nextEntry) > highValString)) {
                    goto completed;
                }
                outRid = container.node.getRID(nextEntry);
                nextEntry++;
                break;
            }
        }

        return;

        completed:
            throw IndexScanCompletedException();
    }

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
    const void BTreeIndex::endScan() {
        if (!scanExecuting) {
            throw ScanNotInitializedException();
        }
    }

//    int validateKeys(Page page) {
//        LeafNodeString node = *(LeafNodeString*) &page;
//        int prev = std::stoi(node.getKey(0));
//        for (int i = 1; i < LeafNodeString::getKeyArraySize() && node.getKey(i) != LeafNodeString::invalidKey(); i++) {
//            int cur = std::stoi(node.getKey(i));
//            if (prev + 1 != cur) {
//                return i;
//            }
//            prev = cur;
//        }
//        return -1;
//    }

}
