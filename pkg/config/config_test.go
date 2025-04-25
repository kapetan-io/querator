package config

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/kapetan-io/querator/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestLoadYamlConfigFile(t *testing.T) {
	for _, tc := range []struct {
		name           string
		configFilePath string
		expectErrCheck func(error) bool
	}{
		{
			name:           "valid generic config yaml",
			configFilePath: "../../test/testyamlconfig/validconfig1.yaml",
			expectErrCheck: nil,
		},
		{
			name:           "error file not exists",
			configFilePath: "../../test/testyamlconfig/does_not_exist.yaml",
			expectErrCheck: func(err error) bool {
				var expectErr ErrFileNotExist
				return errors.As(err, &expectErr)
			},
		},
		{
			name:           "failed yaml read invalid mapping values",
			configFilePath: "../../test/testyamlconfig/invalidconfig1.yaml",
			expectErrCheck: func(err error) bool {
				var expectErr ErrYAMLParse
				return errors.As(err, &expectErr)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFile(tc.configFilePath)
			if tc.expectErrCheck != nil {
				assert.True(t, tc.expectErrCheck(err))
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestConvertYamlConfigToDaemonConfig(t *testing.T) {
	for _, tc := range []struct {
		name           string
		log            *slog.Logger
		config         Config
		expectErrCheck func(error) bool
	}{
		{
			name: "valid config instance to daemon config",
			log:  slog.New(slog.NewTextHandler(os.Stdout, nil)),
			config: Config{
				Backends: []Backend{
					{
						Name:     "inmem-00",
						Driver:   driverTypeMemory,
						Affinity: 0,
					},
				},
				Queues: []Queue{
					{
						Name:                "queue-1",
						DeadQueue:           "queue-dead-1",
						LeaseTimeout:        10 * time.Minute,
						ExpireTimeout:       10 * time.Minute,
						MaxAttempts:         10,
						Reference:           "test",
						RequestedPartitions: 20,
						Partitions: []Partition{
							{
								StorageName: "inmem-00",
								ReadOnly:    false,
								Partition:   0,
							},
						},
					},
				},
			},
		}, {
			name: "unsupported driver type",
			log:  slog.New(slog.NewTextHandler(os.Stdout, nil)),
			expectErrCheck: func(err error) bool {
				var expectErr ErrUnsupportedBackendDriverType
				return errors.As(err, &expectErr)
			},
			config: Config{
				Backends: []Backend{
					{
						Name:     "inmem-00",
						Driver:   "invalid driver type",
						Affinity: 0,
					},
				},
			},
		},
	} {
		dmnCfg, err := tc.config.ToDaemonConfig(context.Background(), tc.log)
		if tc.expectErrCheck != nil {
			assert.True(t, tc.expectErrCheck(err))
			return
		}

		// backend validation
		for i := 0; i < len(tc.config.Backends); i++ {
			assert.Equal(t, dmnCfg.StorageConfig.Backends[0].Name, tc.config.Backends[i].Name)
			assert.EqualValues(t, dmnCfg.StorageConfig.Backends[0].Affinity, tc.config.Backends[i].Affinity)
			// TODO: HOW TO CHECK BACKEND DRIVER TYPE?
		}

		// queue  validation
		for i := 0; i < len(tc.config.Queues); i++ {
			var queueInfo types.QueueInfo
			dmnCfg.StorageConfig.Queues.Get(context.Background(), tc.config.Queues[i].Name, &queueInfo)
			assert.Equal(t, queueInfo.Name, tc.config.Queues[i].Name)
			assert.Equal(t, queueInfo.DeadQueue, tc.config.Queues[i].DeadQueue)
			assert.Equal(t, queueInfo.LeaseTimeout, tc.config.Queues[i].LeaseTimeout)
			assert.Equal(t, queueInfo.ExpireTimeout, tc.config.Queues[i].ExpireTimeout)
			assert.Equal(t, queueInfo.MaxAttempts, tc.config.Queues[i].MaxAttempts)
			assert.Equal(t, queueInfo.Reference, tc.config.Queues[i].Reference)
			assert.Equal(t, queueInfo.RequestedPartitions, tc.config.Queues[i].RequestedPartitions)

			for j := 0; j < len(tc.config.Queues[i].Partitions); j++ {
				// queue partition validation
				assert.Equal(t, queueInfo.PartitionInfo[j].StorageName, tc.config.Queues[j].Partitions[j].StorageName)
				assert.Equal(t, queueInfo.PartitionInfo[j].ReadOnly, tc.config.Queues[j].Partitions[j].ReadOnly)
				assert.Equal(t, queueInfo.PartitionInfo[j].PartitionNum, tc.config.Queues[j].Partitions[j].Partition)
			}
		}

	}
}
