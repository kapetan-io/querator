package service_test

import (
	"context"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUsers(t *testing.T) {
	for _, tc := range []struct {
		Setup NewStorageFunc
		Name  string
	}{
		{
			Name: "InMemory",
			Setup: func() store.Config {
				return setupMemoryStorage(store.Config{})
			},
		},
		{
			Name: "BadgerDB",
			Setup: func() store.Config {
				badger := &badgerTestSetup{Dir: t.TempDir()}
				t.Cleanup(func() {
					badger.Teardown()
				})
				return badger.Setup(store.BadgerConfig{})
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testUsers(t, tc.Setup)
		})
	}
}

func testUsers(t *testing.T, setup NewStorageFunc) {
	// Happy Path: User and APIKey CRUD
	t.Run("UserCreateAndList", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		// Create a user
		var createRes pb.UserCreateResponse
		err := c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "test-user-" + random.String("", 5),
			Email:    "test@example.com",
		}, &createRes)
		require.NoError(t, err)
		require.NotEmpty(t, createRes.Id)

		// List users and verify the user exists
		var listRes pb.UsersListResponse
		err = c.UsersList(ctx, &listRes, nil)
		require.NoError(t, err)
		require.Len(t, listRes.Items, 1)
		assert.Equal(t, createRes.Id, listRes.Items[0].Id)
	})

	t.Run("UserDelete", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		// Create a user
		var createRes pb.UserCreateResponse
		err := c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "delete-user-" + random.String("", 5),
			Email:    "delete@example.com",
		}, &createRes)
		require.NoError(t, err)

		// Delete the user
		err = c.UsersDelete(ctx, &pb.UsersDeleteRequest{Id: createRes.Id})
		require.NoError(t, err)

		// Verify user is deleted
		var listRes pb.UsersListResponse
		err = c.UsersList(ctx, &listRes, nil)
		require.NoError(t, err)
		assert.Empty(t, listRes.Items)
	})

	t.Run("APIKeyCreateAndList", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		// Create a user first
		var userRes pb.UserCreateResponse
		err := c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "apikey-user-" + random.String("", 5),
			Email:    "apikey@example.com",
		}, &userRes)
		require.NoError(t, err)

		// Create an API key
		var keyRes pb.APIKeyCreateResponse
		err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
			UserId: userRes.Id,
			Name:   "test-key",
		}, &keyRes)
		require.NoError(t, err)
		require.NotEmpty(t, keyRes.Id)
		require.NotEmpty(t, keyRes.Key)
		require.NotEmpty(t, keyRes.Prefix)

		// List API keys
		var listRes pb.APIKeysListResponse
		err = c.APIKeysList(ctx, &listRes, nil)
		require.NoError(t, err)
		require.Len(t, listRes.Items, 1)
		assert.Equal(t, keyRes.Id, listRes.Items[0].Id)
		assert.Equal(t, userRes.Id, listRes.Items[0].UserId)
	})

	t.Run("APIKeyListByUser", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		// Create two users
		var user1Res pb.UserCreateResponse
		err := c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "user1-" + random.String("", 5),
		}, &user1Res)
		require.NoError(t, err)

		var user2Res pb.UserCreateResponse
		err = c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "user2-" + random.String("", 5),
		}, &user2Res)
		require.NoError(t, err)

		// Create API key for user1
		var key1Res pb.APIKeyCreateResponse
		err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
			UserId: user1Res.Id,
			Name:   "key-for-user1",
		}, &key1Res)
		require.NoError(t, err)

		// Create API key for user2
		var key2Res pb.APIKeyCreateResponse
		err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
			UserId: user2Res.Id,
			Name:   "key-for-user2",
		}, &key2Res)
		require.NoError(t, err)

		// List API keys for user1 only
		var listRes pb.APIKeysListResponse
		err = c.APIKeysListByUser(ctx, user1Res.Id, &listRes, nil)
		require.NoError(t, err)
		require.Len(t, listRes.Items, 1)
		assert.Equal(t, key1Res.Id, listRes.Items[0].Id)
	})

	t.Run("APIKeyDelete", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		// Create a user
		var userRes pb.UserCreateResponse
		err := c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "apikey-delete-user-" + random.String("", 5),
		}, &userRes)
		require.NoError(t, err)

		// Create an API key
		var keyRes pb.APIKeyCreateResponse
		err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
			UserId: userRes.Id,
			Name:   "delete-key",
		}, &keyRes)
		require.NoError(t, err)

		// Delete the API key
		err = c.APIKeysDelete(ctx, &pb.APIKeysDeleteRequest{Id: keyRes.Id})
		require.NoError(t, err)

		// Verify API key is deleted
		var listRes pb.APIKeysListResponse
		err = c.APIKeysList(ctx, &listRes, nil)
		require.NoError(t, err)
		assert.Empty(t, listRes.Items)
	})

	t.Run("UserDeleteCascadeAPIKeys", func(t *testing.T) {
		d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
		defer d.Shutdown(t)

		// Create a user
		var userRes pb.UserCreateResponse
		err := c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "cascade-user-" + random.String("", 5),
		}, &userRes)
		require.NoError(t, err)

		// Create API keys for the user
		var key1Res pb.APIKeyCreateResponse
		err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
			UserId: userRes.Id,
			Name:   "cascade-key-1",
		}, &key1Res)
		require.NoError(t, err)

		var key2Res pb.APIKeyCreateResponse
		err = c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
			UserId: userRes.Id,
			Name:   "cascade-key-2",
		}, &key2Res)
		require.NoError(t, err)

		// Delete the user (should cascade delete API keys)
		err = c.UsersDelete(ctx, &pb.UsersDeleteRequest{Id: userRes.Id})
		require.NoError(t, err)

		// Verify API keys are deleted
		var listRes pb.APIKeysListResponse
		err = c.APIKeysList(ctx, &listRes, nil)
		require.NoError(t, err)
		assert.Empty(t, listRes.Items)
	})

	// Error Tests
	t.Run("Errors", func(t *testing.T) {
		t.Run("UsersCreate", func(t *testing.T) {
			t.Run("EmptyUsername", func(t *testing.T) {
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
				defer d.Shutdown(t)

				var res pb.UserCreateResponse
				err := c.UsersCreate(ctx, &pb.UserCreateRequest{
					Username: "",
				}, &res)
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "username is invalid")
			})

			t.Run("DuplicateUsername", func(t *testing.T) {
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
				defer d.Shutdown(t)

				username := "duplicate-user-" + random.String("", 5)

				// Create first user
				var res1 pb.UserCreateResponse
				err := c.UsersCreate(ctx, &pb.UserCreateRequest{
					Username: username,
				}, &res1)
				require.NoError(t, err)

				// Try to create user with same username
				var res2 pb.UserCreateResponse
				err = c.UsersCreate(ctx, &pb.UserCreateRequest{
					Username: username,
				}, &res2)
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "username is already taken")
			})
		})

		t.Run("UsersDelete", func(t *testing.T) {
			t.Run("NotExist", func(t *testing.T) {
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
				defer d.Shutdown(t)

				err := c.UsersDelete(ctx, &pb.UsersDeleteRequest{
					Id: "non-existent-id",
				})
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "user does not exist")
			})
		})

		t.Run("APIKeysCreate", func(t *testing.T) {
			t.Run("EmptyUserID", func(t *testing.T) {
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
				defer d.Shutdown(t)

				var res pb.APIKeyCreateResponse
				err := c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
					UserId: "",
					Name:   "test-key",
				}, &res)
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "user_id is invalid")
			})

			t.Run("UserNotExist", func(t *testing.T) {
				d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})
				defer d.Shutdown(t)

				var res pb.APIKeyCreateResponse
				err := c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
					UserId: "non-existent-user",
					Name:   "test-key",
				}, &res)
				require.Error(t, err)
				var duhErr duh.Error
				require.ErrorAs(t, err, &duhErr)
				assert.Contains(t, duhErr.Message(), "user does not exist")
			})
		})
	})
}

func TestUsersContext(t *testing.T) {
	d, c, _ := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setupMemoryStorage(store.Config{})})
	defer d.Shutdown(t)

	t.Run("CancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var res pb.UserCreateResponse
		err := c.UsersCreate(ctx, &pb.UserCreateRequest{
			Username: "test-user",
		}, &res)
		require.Error(t, err)
	})
}
