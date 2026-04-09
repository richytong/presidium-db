class BTreeNode {
  constructor(t, leaf = true) {
    this.t = t;           // Minimum degree
    this.keys = [];      // Array of keys
    this.children = [];  // Array of child pointers
    this.leaf = leaf;    // Boolean: true if leaf, false if internal
  }

  // Search for a key in this subtree
  search(k) {
    let i = 0;
    while (i < this.keys.length && k > this.keys[i]) {
      i++;
    }

    if (this.keys[i] === k) return this;
    if (this.leaf) return null;

    return this.children[i].search(k);
  }
}

class BTree {
  constructor(t) {
    this.root = null;
    this.t = t; // Minimum degree
  }

  insert(k) {
    if (!this.root) {
      this.root = new BTreeNode(this.t, true); // root is first child
      this.root.keys[0] = k;
      return;
    }

    // split root
    if (this.root.keys.length === 2 * this.t - 1) { // t2 -> 3 -> 1 - 1 - 1, t3 -> 5 -> 2 - 1 - 2, t4 -> 7 -> 3 - 1 - 3
      const s = new BTreeNode(this.t, false); // new internal node (becomes root)
      s.children[0] = this.root; // root becomes left child (leaf) of internal node, retains first t of children and first t - 1 of keys
      this.splitChild(s, 0, this.root); // split full left child (prior root), index 0 of new internal node becomes middle key
      
      // insert k
      let i = 0;
      if (s.keys[0] < k) i++; // s.keys[0] is middle key, insert k into left child node or right child node
      this.insertNonFull(s.children[i], k);
      this.root = s;
    } else {
      this.insertNonFull(this.root, k);
    }
  }

  // Internal helper to insert into a non-full node
  insertNonFull(node, k) {
    let i = node.keys.length - 1;

    if (node.leaf) {
      // Insert key into sorted leaf
      while (i >= 0 && node.keys[i] > k) {
        node.keys[i + 1] = node.keys[i];
        i--;
      }
      node.keys[i + 1] = k;
    } else {
      // Find child to descend into
      while (i >= 0 && node.keys[i] > k) i--;
      i++; // descend into the first child where the key was greater than k (least right child), or the leftmost child if no key was greater than k (leftmost child)

      // i is index of child to descend into

      if (node.children[i].keys.length === 2 * this.t - 1) { // child to descend into is full, split child
        // node is not full, node changes key at i to middle key of child to descend into
        this.splitChild(node, i, node.children[i]);
        if (node.keys[i] < k) i++;
      }
      this.insertNonFull(node.children[i], k);
    }
  }

  // Split a full child node, i becomes index of middle key of fullChild
  splitChild(parent, i, fullChild) {
    const t = this.t;
    const newNode = new BTreeNode(t, fullChild.leaf); // latter child node
    
    // Move the last (t-1) keys of fullChild to newNode
    newNode.keys = fullChild.keys.splice(t, t - 1);
    
    // Move the last t children if not a leaf
    if (!fullChild.leaf) {
      newNode.children = fullChild.children.splice(t, t);
    }

    // Move middle key of fullChild up to parent
    const middleKey = fullChild.keys.pop();
    parent.keys.splice(i, 0, middleKey);
    parent.children.splice(i + 1, 0, newNode);
  }
}

const btree = new BTree(2)
btree.insert(2)
btree.insert(8)
btree.insert(9)
btree.insert(11)
btree.insert(13)
btree.insert(14)
btree.insert(15)
btree.insert(16)
btree.insert(20)
// btree.insert(12)
// btree.insert(10)
// btree.insert(21)
// btree.insert(22)
// btree.insert(23)
// btree.insert(24)
// btree.insert(25)
// btree.insert(26)
// btree.insert(27)
// btree.insert(28)
// btree.insert(29)
// btree.insert(30)

console.log(JSON.stringify(btree, null, 2))

module.exports = BTree
