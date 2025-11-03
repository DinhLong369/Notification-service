package app

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
)

type LoggingConfig struct {
	Type       string `env:"type" `
	ServerName string `env:"server_name"`
}

func (log *LoggingConfig) Setup() {
	logrus.SetOutput(os.Stdout)
	if log.Type == "production" {
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetLevel(logrus.DebugLevel)
	}
	my_formater := &logrus.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := path.Base(f.File)
			package_name := f.Func.Name()
			spliter_1 := strings.Split(package_name, ".")
			if len(spliter_1) > 0 {
				spliter_2 := strings.Split(spliter_1[0], "/")
				if len(spliter_2) > 1 {
					paths := spliter_2[1:]
					fullpath := strings.Join(paths, "/")
					return fmt.Sprintf("%s/%s:%d", fullpath, filename, f.Line), fmt.Sprintf("%s()", f.Function)
				}
			}
			return fmt.Sprintf("%s:%d", filename, f.Line), fmt.Sprintf("%s()", f.Function)
		},
	}
	logrus.SetFormatter(my_formater)
	logrus.SetReportCaller(true)

}
