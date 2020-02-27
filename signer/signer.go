package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)
const BufferLen  = MaxInputDataLen

const MultiHashConst  = 6

func getCrc32(data string, ch chan string){
	ch <- DataSignerCrc32(data)
}

func getMD5(data string) string{
	return DataSignerMd5(data)
}

//функция конвертирования
func toString(in interface{})  string{
	switch in.(type) {
	case int: //для th в multihash
		return strconv.FormatInt(int64(in.(int)), 10)
	case string:
		return in.(string)
	case chan interface{}:
		return fmt.Sprintf("%v", in) //не самый элегантный метод
	default:
		panic(fmt.Sprintf("error ocused while converting %T to string", in))
	}
}

//на вход: канал на чтение, канал на запись
func SingleHash (in, out chan interface{})  {
	swg := &sync.WaitGroup{}

	//по итогам TestSigner: цикл необходим
	for i := range in {
		swg.Add(1)
		//нужно получить два результата
		//для каждой дата
		data := toString(i)

		left := make(chan string)
		right := make(chan string)

		go getCrc32(data, left)
		go getCrc32(getMD5(data), right)

		go func() {
			defer swg.Done()
			out <- <-left + "~" + <-right
		}()
	}
	swg.Wait()
}

func MultiHash (in, out chan interface{}) {
	//тоже группа
	hwg := &sync.WaitGroup{}

	for i := range in {
		hwg.Add(1)

		var data string
		data = toString(i)
		//6 каналов со своими вычислениями
		var toKeep = make([]chan string, MultiHashConst)

		for th := range toKeep {
			toKeep[th] = make(chan string)
			go getCrc32(toString(th)+data, toKeep[th])
		}

		var result string
		go func() {
			defer hwg.Done()
			for th := range toKeep {
				result += <-toKeep[th]
			}
			out <- result
		}()
	}
	hwg.Wait()
}

//на вход: канал, откуда получаем
//на выход: канал, куда записываем
func CombineResults(in, out chan interface{}) {
	var toSort []string

	//получили из канала все строки
	for s := range in{
		toSort = append(toSort, toString(s))
	}
	//отсортировали
	sort.Strings(toSort)

	//объединили через _
	var result = strings.Join(toSort, "_")
	//записали в канал
	out <- result
}

func doJob(toDo job, in, out chan interface{}, jwg *sync.WaitGroup){
	defer jwg.Done() //убрали одну задачу
	toDo(in, out)
	close(out)
}

//на вход: список работ
//на выход: ничего?
func ExecutePipeline(jobs ... job) {
	//конвеер
	jwg := &sync.WaitGroup{} //считает для jobs
	//взяли информацию
	in := make(chan interface{}, BufferLen)
	close(in)

	// out одной функции это in другой
	//индекс нам не нужен
	for _, job := range jobs{
		jwg.Add(1) //добавили одну задачу
		out := make(chan interface{}, BufferLen) //открыли
		go doJob(job, in, out, jwg)
		//close(out) //закрыли //TestByIlia
		in = out
	}
	jwg.Wait() //ждем окончания всей группы
}

