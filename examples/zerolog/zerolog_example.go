package main

import (
	"context"
	"fmt"
	"github.com/gogearbox/gearbox"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/sweemingdow/log_remote_writer/pkg/writer/tcpwriter"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func init() {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
}

func main() {
	gb := gearbox.New()

	// http writer test
	//remoteWriter := httpwriter.New(httpwriter.HttpRemoteConfig{
	//	Url:                       "http://192.168.1.155:9088", // my fluent-bit server
	//	Workers:                   16,
	//	BatchQuantitativeSize:     100,
	//	QueueSize:                 500,
	//	Debug:                     false,
	//	DisplayMonitorIntervalSec: 15, // display monitor metrics
	//})

	// tcp writer test
	// To switch from http to tcp implementation, the only thing to do is to change the configuration
	remoteWriter := tcpwriter.New(tcpwriter.TcpRemoteConfig{
		Host:                      "192.168.1.155", // my fluent-bit server
		Port:                      5170,
		QueueSize:                 2000,
		Debug:                     false,
		DisplayMonitorIntervalSec: 15, // display monitor metrics
	})

	rootLogger := zerolog.New(zerolog.MultiLevelWriter(
		/*os.Stdout,*/
		remoteWriter,
	)).With().Timestamp().Int("pid", os.Getpid()).Logger()

	gb.Get(
		"/test/display",
		func(ctx gearbox.Context) {
			level := ctx.Query("level")
			if len(level) == 0 {
				level = "debug"
			}

			ll, err := zerolog.ParseLevel(level)
			if err != nil {
				ctx.Status(http.StatusBadRequest).SendString("unknown level:" + level)
				return
			}

			guc, _ := strconv.Atoi(ctx.Query("guc"))

			count, _ := strconv.Atoi(ctx.Query("count"))

			logger := rootLogger.Level(ll).With().Str("logger", "testLogger").Logger()

			wg := sync.WaitGroup{}
			wg.Add(guc)

			start := time.Now()
			for i := 0; i < guc; i++ {
				i := i
				go func() {
					defer wg.Done()

					for j := 0; j < count/guc; j++ {
						seq := fmt.Sprintf("%d-%d", i, j)

						{
							lg := logger.With().Str("org", "biz_sys").Str("seq", seq).Int("uid", i).Logger()

							lg.Trace().Msg("this is a trace msg")
							lg.Debug().Msg("this is a debug msg")
							lg.Info().Msg("this is a info msg")
							lg.Warn().Msg("this is a warn msg")
							lg.Error().Msg("this is a error msg")
						}

						{
							lg := logger.With().Str("org", "im_sys").Str("seq", seq).Int("uid", j).Logger()

							lg.Trace().Msg("this is a trace msg")
							lg.Debug().Msg("this is a debug msg")
							lg.Info().Msg("this is a info msg")
							lg.Warn().Msg("this is a warn msg")
							lg.Error().Msg("this is a error msg")
						}
					}
				}()

			}

			wg.Wait()

			info := fmt.Sprintf("send log to remote completed, cnt:%d, guc:%d, level:%s, took:%v\n", count, guc, level, time.Since(start))
			log.Printf(info)
			ctx.SendString(info)
		},
	)

	go func() {
		// start web server
		if e := gb.Start(":9191"); e != nil {
			panic(e)
		}
	}()

	var errChan = make(chan error, 1)

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
		errChan <- fmt.Errorf("%s", <-sigChan)
	}()

	exitErr := <-errChan
	log.Printf("receive signal:%v\n", exitErr)

	// stop gracefully
	err := remoteWriter.Stop(context.Background())
	if err != nil {
		log.Printf("stop remote writer failed:%v\n", err)
	}

	log.Println("exit now!")

	time.Sleep(50 * time.Millisecond)
}
