package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"gopkg.in/go-playground/validator.v9"
	"log"
	"net/http"
)

type App struct{
	Router *mux.Router
	Middlewares *Middleware
	Config *Env
}

type shortenReq struct{
	URL string `json:"url" validate"nonzero"`
	ExpirationInMinutes int64 `json:"expiration_in_minutes" validate:"min=0"`
}

type shortlinkResp struct{
	Shortlink string `json:"shortlink"`
}

func (a *App) Initialize(e *Env){
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	a.Config = e
	a.Router = mux.NewRouter()
	a.Middlewares = &Middleware{}
	a.InitializeRoutes()
}
func (a *App) InitializeRoutes(){
	m:=alice.New(a.Middlewares.LoggingHandler,a.Middlewares.RecoverHandler)
	a.Router.Handle("/api/shorten",m.ThenFunc(a.createShortlink)).Methods("POST")
	a.Router.Handle("/api/info",m.ThenFunc(a.getShortlinkInfo)).Methods("GET")
	a.Router.Handle("/{shortlink:[a-zA-Z0-9]{1,11}}",m.ThenFunc(a.redirect)).Methods("GET")
}
func  (a *App) createShortlink(w http.ResponseWriter,r *http.Request){
	var req shortenReq
	if err := json.NewDecoder(r.Body).Decode(&req);err!=nil{
		respondWithError(w,StatusError{Code:http.StatusBadRequest,Err:fmt.Errorf("parse parameters failed %v",r.Body)})
		return
	}
	if err:= validator.New().Struct(req);err!=nil{
		respondWithError(w,StatusError{Code:http.StatusBadRequest,Err:fmt.Errorf("validation error %v",r.Body)})
		return
	}
	defer r.Body.Close()

	s,err:=a.Config.S.Shorten(req.URL,req.ExpirationInMinutes)
	if err!=nil{
		respondWithError(w,StatusError{Code:http.StatusBadRequest,Err:err})
	}else{
		respondWithJson(w,http.StatusCreated,shortlinkResp{Shortlink:s})
	}

}
func  (a *App) getShortlinkInfo(w http.ResponseWriter,r *http.Request){
	vals:=r.URL.Query()
	s:=vals.Get("shortlink")
	defer r.Body.Close()

	d,err:=a.Config.S.ShortlinkInfo(s)
	if err!=nil{
		respondWithError(w,StatusError{Code:http.StatusBadRequest,Err:err})
	}else{
		respondWithJson(w,http.StatusOK,d)
	}
}
func  (a *App) redirect(w http.ResponseWriter,r *http.Request){
	vars :=mux.Vars(r)
	fmt.Printf("%s\n",vars["shortlink"])
	u,err:=a.Config.S.Unshorten(vars["shortlink"])
	if err!=nil{
		respondWithError(w,StatusError{Code:http.StatusBadRequest,Err:err})
	}else{
		http.Redirect(w,r,u,http.StatusTemporaryRedirect)
	}
}
func (a *App) Run(addr string){
	log.Fatal(http.ListenAndServe(addr,a.Router))
}

func respondWithError(w http.ResponseWriter,err Error){
	switch e:=err.(type){
	case Error:
		log.Printf("HTTP %d -%s",e.Status(),e.Error())
		respondWithJson(w,e.Status(),e.Error())
	default:
		respondWithJson(w,http.StatusInternalServerError,http.StatusInternalServerError)
	}
}

func respondWithJson(writer http.ResponseWriter, code int,payload interface{}) {
	resp,_ :=json.Marshal(payload)
	writer.Header().Set("Content-Type","application/json")
	writer.WriteHeader(code)
	writer.Write(resp)
}