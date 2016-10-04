package treeserve

import "testing"

func TestTreeNode(t *testing.T) {
	testNode := &TreeNode{Name: "testNode"}
	data, err := testNode.Marshal(nil)
	if err != nil {
		t.Errorf("failed to marshal treenode")
	}

	checkTestNode := &TreeNode{}
	_, err = checkTestNode.Unmarshal(data)
	if err != nil {
		t.Errorf("failed to unmarshal treenode")
	}

	if *checkTestNode != *testNode {
		t.Errorf("unmarshalled treenode did not match: %v != %v", *checkTestNode, *testNode)
	}
}

func TestTreeNodeMarshalUnmarshaler(t *testing.T) {
	testNode := &TreeNode{Name: "testNode"}
	testNodeData, err := testNode.MarshalBinary()
	if err != nil {
		t.Errorf("failed to binary marshal treenode")
	}

	checkTestNode := &TreeNode{}
	checkTestNode.UnmarshalBinary(testNodeData)

	if *checkTestNode != *testNode {
		t.Errorf("binary unmarshalled treenode did not match: %v != %v", *checkTestNode, *testNode)
	}
}
