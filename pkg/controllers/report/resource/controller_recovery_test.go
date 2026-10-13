package resource

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/kyverno/kyverno/pkg/clients/dclient"
	"gotest.tools/v3/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

var (
	testGVR = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	testGVK = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
)

// listResult is one canned response for the List call, in order.
type listResult struct {
	resourceVersion string
	names           []string
	err             error
}

// fakeWatch is the watcher handed out for a Watch call, with the resource version it started from.
type fakeWatch struct {
	*watch.RaceFreeFakeWatcher
	resourceVersion string
}

// expire fails the watch the way the apiserver does once its resource version is too old.
func (w fakeWatch) expire() {
	status := apierrors.NewResourceExpired("too old resource version: " + w.resourceVersion).ErrStatus
	w.Error(&status)
}

// fakeResourceClient serves a scripted sequence of Lists and hands out a fresh
// watcher for every Watch call, in order.
type fakeResourceClient struct {
	*dynamicfake.FakeDynamicClient

	mu      sync.Mutex
	lists   []listResult
	listed  int
	watches chan fakeWatch
}

func newFakeResourceClient(lists ...listResult) *fakeResourceClient {
	c := &fakeResourceClient{
		FakeDynamicClient: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
			testGVR: "ConfigMapList",
		}),
		lists:   lists,
		watches: make(chan fakeWatch, 8),
	}
	c.PrependReactor("list", "*", func(k8stesting.Action) (bool, runtime.Object, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		result := c.lists[min(c.listed, len(c.lists)-1)]
		c.listed++
		if result.err != nil {
			return true, nil, result.err
		}
		list := &unstructured.UnstructuredList{}
		list.SetResourceVersion(result.resourceVersion)
		for _, name := range result.names {
			list.Items = append(list.Items, configMap(name))
		}
		return true, list, nil
	})
	c.PrependWatchReactor("*", func(action k8stesting.Action) (bool, watch.Interface, error) {
		w := fakeWatch{
			RaceFreeFakeWatcher: watch.NewRaceFreeFake(),
			resourceVersion:     action.(k8stesting.WatchAction).GetWatchRestrictions().ResourceVersion,
		}
		select {
		case c.watches <- w:
		default: // nobody is waiting for more watches, don't block the client
		}
		return true, w, nil
	})
	return c
}

// nextWatch returns the watcher handed out for the next Watch call.
func (c *fakeResourceClient) nextWatch(t *testing.T) fakeWatch {
	t.Helper()
	select {
	case w := <-c.watches:
		return w
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for a watch")
		return fakeWatch{}
	}
}

func configMap(name string) unstructured.Unstructured {
	obj := unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace("default")
	obj.SetName(name)
	obj.SetUID(types.UID("uid-" + name))
	return obj
}

// startTestWatcher starts watching testGVR the way updateDynamicWatchers does.
func startTestWatcher(t *testing.T, c *controller) {
	t.Helper()
	c.lock.Lock()
	defer c.lock.Unlock()
	w, err := c.startWatcher(t.Context(), logr.Discard(), testGVR, testGVK)
	assert.NilError(t, err)
	c.dynamicWatchers[testGVR] = w
}

// recordDeletes returns a func listing the uids the cache notified as deleted so far.
func recordDeletes(c *controller) func() []types.UID {
	var mu sync.Mutex
	var deleted []types.UID
	c.AddEventHandler(func(eventType EventType, uid types.UID, _ schema.GroupVersionKind, _ Resource) {
		if eventType != Deleted {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		deleted = append(deleted, uid)
	})
	return func() []types.UID {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(deleted)
	}
}

// awaitCached waits until the cache holds a hash for uid.
func awaitCached(t *testing.T, c *controller, uid types.UID) {
	t.Helper()
	err := wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, 15*time.Second, true, func(context.Context) (bool, error) {
		_, _, _, exists := c.GetResourceHash(uid)
		return exists, nil
	})
	assert.NilError(t, err, "timed out waiting for %s to be cached", uid)
}

// A watch that fails with "resource version too old" must resume from a fresh list, so
// resources created or deleted while it was down are reflected in the cache.
func TestWatcherRestartsFromFreshListOnExpiredResourceVersion(t *testing.T) {
	client := newFakeResourceClient(
		listResult{resourceVersion: "100", names: []string{"a", "b"}},
		listResult{resourceVersion: "200", names: []string{"b", "c"}},
	)
	c := &controller{
		client:          dclient.NewFakeClientWithDisco(client, nil, nil),
		dynamicWatchers: map[schema.GroupVersionResource]*watcher{},
	}
	deleted := recordDeletes(c)
	startTestWatcher(t, c)

	w := client.nextWatch(t)
	assert.Equal(t, w.resourceVersion, "100")
	w.expire()

	// the restarted watch resumes from the fresh list, not from the expired resource version
	assert.Equal(t, client.nextWatch(t).resourceVersion, "200")
	awaitCached(t, c, "uid-c")
	_, _, _, exists := c.GetResourceHash("uid-b")
	assert.Assert(t, exists)
	_, _, _, exists = c.GetResourceHash("uid-a")
	assert.Assert(t, !exists, "a was deleted while the watch was down")
	assert.DeepEqual(t, deleted(), []types.UID{"uid-a"})
}

// A restart whose list fails must be retried rather than leave the gvr on a dead watcher.
func TestWatcherRestartIsRetriedUntilListSucceeds(t *testing.T) {
	client := newFakeResourceClient(
		listResult{resourceVersion: "100", names: []string{"a"}},
		listResult{err: apierrors.NewServiceUnavailable("apiserver is restarting")},
		listResult{resourceVersion: "200", names: []string{"a", "b"}},
	)
	c := &controller{
		client:          dclient.NewFakeClientWithDisco(client, nil, nil),
		dynamicWatchers: map[schema.GroupVersionResource]*watcher{},
	}
	startTestWatcher(t, c)

	client.nextWatch(t).expire()

	assert.Equal(t, client.nextWatch(t).resourceVersion, "200")
	awaitCached(t, c, "uid-b")
}

// A watcher dropped while waiting to restart, e.g. because no policy matches its kind anymore, must stay stopped.
func TestRemovedWatcherIsNotRestarted(t *testing.T) {
	client := newFakeResourceClient(listResult{resourceVersion: "100", names: []string{"a"}})
	c := &controller{
		client:          dclient.NewFakeClientWithDisco(client, nil, nil),
		dynamicWatchers: map[schema.GroupVersionResource]*watcher{},
	}
	startTestWatcher(t, c)

	client.nextWatch(t).expire()
	c.lock.Lock()
	delete(c.dynamicWatchers, testGVR)
	c.lock.Unlock()

	select {
	case <-client.watches:
		t.Fatal("a removed watcher was restarted")
	case <-time.After(3 * time.Second):
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.Equal(t, client.listed, 1)
}
