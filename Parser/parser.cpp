#include <iostream>

#include "clang/Tooling/CommonOptionsParser.h"
#include "clang/Tooling/Tooling.h"
#include "clang/Frontend/FrontendActions.h"
#include "clang/Rewrite/Core/Rewriter.h"
#include "clang/AST/AST.h"
#include "clang/AST/ASTConsumer.h"
#include "clang/ASTMatchers/ASTMatchers.h"
#include "clang/ASTMatchers/ASTMatchFinder.h"
#include "clang/Frontend/CompilerInstance.h"
#include "llvm/Support/raw_ostream.h"
#include <string>
#include<map>
using namespace clang;
using namespace clang::tooling;
using namespace clang::ast_matchers;
class MyASTConsumer;
class MyFrontEndAction;
class Parser;
class PrintHandler;



class MetaData{
    public:
    std::map<std::string ,std::string> transformedFuncs;

    void addTranformedFunct(std::string source,std::string  transformed){
        transformedFuncs[source]=transformed;
    }

    void printAllTransformedFunctions(){
        for(auto &func:transformedFuncs){
            llvm::outs()<<"\n\nTransformed  function "<<func.first <<" into function "<<func.second<<"\n\n";
        }
    }

    

};
class PrintHandler : public MatchFinder::MatchCallback {
private:
    Rewriter &rewriter;
    MetaData &metadata;
    

public:
    PrintHandler(Rewriter &rewriter,MetaData &MetaData):rewriter(rewriter),metadata(MetaData){
        
    }
    void run(const MatchFinder::MatchResult &Result) override {
        const FunctionDecl *func =
            Result.Nodes.getNodeAs<FunctionDecl>("func");

        if (!func)
            return;

      
        

    }
};



class VarHandler : public MatchFinder::MatchCallback {
    Rewriter &rewriter;
    MetaData &metadata;


public:
    VarHandler(Rewriter &rewriter, MetaData &metadata): rewriter(rewriter),metadata(metadata){};

    void run(const MatchFinder::MatchResult &Result) override {
        const VarDecl *var =
            Result.Nodes.getNodeAs<VarDecl>("var");

        if (!var)
            return;

      
        // llvm::outs() << "Found var: " << var->getNameAsString() << "\n";
        for (const Attr *A : var->attrs()) {
            if (const auto *AA = dyn_cast<AnnotateAttr>(A)) {
                llvm::outs() << "Annotation: "
                            << AA->getAnnotation() 
                            <<" at variable "
                            <<var->getNameAsString();
            }
        }
        
    }
};




class CallExprHandler : public MatchFinder::MatchCallback {
    Rewriter &rewriter;
    MetaData &metadata;

public:
    CallExprHandler(Rewriter &rewriter, MetaData &metadata)
        : rewriter(rewriter), metadata(metadata) {}

    void run(const MatchFinder::MatchResult &Result) override {
        const CallExpr *call =
            Result.Nodes.getNodeAs<CallExpr>("call");

        if (!call)
            return;

        const FunctionDecl *FD = call->getDirectCallee();
        if (!FD)
            return;

        std::string name = FD->getNameAsString();

        auto it = metadata.transformedFuncs.find(name);
        if (it == metadata.transformedFuncs.end())
            return;

        const std::string &newName = it->second;

        // get callee expression (function name part only)
        const Expr *callee = call->getCallee();

        SourceRange calleeRange(
            callee->getBeginLoc(),
            callee->getEndLoc()
        );

        rewriter.ReplaceText(calleeRange, newName);
    }
};



class FunctionHandler : public MatchFinder::MatchCallback {
    Rewriter &rewriter;
    MetaData &metadata;


public:
    FunctionHandler(Rewriter &rewriter, MetaData &metadata): rewriter(rewriter),metadata(metadata){};

    void run(const MatchFinder::MatchResult &Result) override {
        const FunctionDecl *func =
            Result.Nodes.getNodeAs<FunctionDecl>("func");

        if (!func)
            return;

      
        int vcpus = 1;
        bool has_annotation = false;

        for (const Attr *A : func->attrs()) {
            if (const auto *AA = dyn_cast<AnnotateAttr>(A)) {
                std::string annotation = AA->getAnnotation().str();
                llvm::outs() << "Annotation: " << annotation << " at function " << func->getNameAsString() << "\n";
                if (annotation.find("vcpus:") == 0) {
                    vcpus = std::stoi(annotation.substr(6));
                    has_annotation = true;
                } else if (annotation.find("goat:") == 0) {
                    has_annotation = true;
                }
            }
        }

        // Only transform if it has a relevant annotation
        if (!has_annotation) {
            return;
        }

        std::string sourceFunction = func->getNameAsString();
        std::string returnType = func->getReturnType().getAsString();
        std::string functionName = sourceFunction + "_generated";

        // Build parameter list and extract param names for execute_fxn
        std::string params;
        std::string data_param = "NULL";
        std::string size_param = "0";

        for (unsigned i = 0; i < func->getNumParams(); ++i) {
            const ParmVarDecl *P = func->getParamDecl(i);
            std::string paramName = P->getNameAsString();
            
            if (i == 0) data_param = paramName;
            if (i == 1) size_param = paramName;

            params += P->getType().getAsString() + " " + paramName;
            if (i + 1 < func->getNumParams()) params += ", ";
        }

        // Create new function text that calls execute_fxn
        std::string newFunction =
            "\n" + returnType + " " + functionName + "(" + params + ") {\n"
            "    execute_fxn(" + data_param + ", " + size_param + ", \"" + sourceFunction + "\", " + std::to_string(vcpus) + ");\n"
            "}\n";

        // Insert at end of file
        clang::SourceLocation endOfFile = rewriter.getSourceMgr().getLocForEndOfFile(rewriter.getSourceMgr().getMainFileID());
        rewriter.InsertText(endOfFile, newFunction, true, true);

        //update metadata
        metadata.addTranformedFunct(sourceFunction, functionName);
    }
};








class MyASTConsumer : public ASTConsumer{
    private:
    MetaData &MymetaData;
    Rewriter &MyRewriter;
    PrintHandler printhandler;
    VarHandler varHandler;
    FunctionHandler FunctionHandler;
    CallExprHandler CallExprHandler;
    MatchFinder FunctionMatcher;
    MatchFinder VariableMatcher;
    MatchFinder CallExprMatcher;


   

   

    public:
    MyASTConsumer(Rewriter &rewriter,MetaData &metadata): MyRewriter(rewriter),
    MymetaData(metadata),
    printhandler(this->MyRewriter,this->MymetaData),
    varHandler(this->MyRewriter,this->MymetaData),
    FunctionHandler(this->MyRewriter,this->MymetaData),
    CallExprHandler(this->MyRewriter,this->MymetaData){
       
        FunctionMatcher.addMatcher(
            //match functions
                functionDecl(isDefinition()).bind("func"),
                 &printhandler

        );

        VariableMatcher.addMatcher(
            varDecl().bind("var"),
            &varHandler
        );
        FunctionMatcher.addMatcher(
            functionDecl(isDefinition()).bind("func"),
            &FunctionHandler
        );

        CallExprMatcher.addMatcher(
            callExpr().bind("call"),
            &CallExprHandler
        );



    }

    //start parsing of AST
    void HandleTranslationUnit(ASTContext &context) override{
        //traverse ast and detect functions
        FunctionMatcher.matchAST(context);
        //traverse ast and detect variables
        MymetaData.printAllTransformedFunctions();
        CallExprMatcher.matchAST(context);

        // Generate the matcher function
        std::string matcherCode = "\n\ntypedef void *(*fn)(void *);\n\n";
        matcherCode += "fn matcher(char *name) {\n";
        
        for (const auto& pair : MymetaData.transformedFuncs) {
            matcherCode += "    if (strcmp(name, \"" + pair.first + "\") == 0) {\n";
            matcherCode += "        return (fn)" + pair.first + ";\n";
            matcherCode += "    }\n";
        }
        
        matcherCode += "    return NULL;\n";
        matcherCode += "}\n";

        clang::SourceLocation endOfFile = MyRewriter.getSourceMgr().getLocForEndOfFile(MyRewriter.getSourceMgr().getMainFileID());
        MyRewriter.InsertText(endOfFile, matcherCode, true, true);
    }




};






 class MyFrontEndAction:public ASTFrontendAction{
    private : 
    Rewriter MyRewriter;
    MetaData metaData;


  

    std::unique_ptr<ASTConsumer> CreateASTConsumer(CompilerInstance &CI,StringRef file) override{
        MyRewriter.setSourceMgr(CI.getSourceManager(),CI.getLangOpts());

        return std::make_unique<MyASTConsumer>(MyRewriter,metaData);

    }


     void EndSourceFileAction() override {

        SourceManager &SM = MyRewriter.getSourceMgr();

        std::string originalFile =
            SM.getFileEntryForID(SM.getMainFileID())
              ->getName()
              .str();

        std::string outputFile =
            originalFile + "_parsed.c";

        std::error_code EC;

        llvm::raw_fd_ostream outFile(
            outputFile,
            EC,
            llvm::sys::fs::OF_Text
        );

        if (EC) {
            llvm::errs()
                << "Cannot open output file: "
                << EC.message()
                << "\n";
            return;
        }

        MyRewriter.getEditBuffer(
            SM.getMainFileID()
        ).write(outFile);

        llvm::outs()
            << "Written rewritten file to: "
            << outputFile
            << "\n";
};
 };



class Parser{

  

    public:
    Parser(){

    }
    


   



   
    int init(int argc, const char ** argv){
        static llvm::cl::OptionCategory ToolCategory("ast-tool");
         auto ExpectedParser=CommonOptionsParser::create(argc,argv,ToolCategory);
        if(!ExpectedParser){
            llvm::errs() << ExpectedParser.takeError();
            return 1;
        }
        CommonOptionsParser &OptionsParser=ExpectedParser.get();
        ClangTool Tool(
            OptionsParser.getCompilations(),
            OptionsParser.getSourcePathList()
        );
    
        return Tool.run(
            newFrontendActionFactory<MyFrontEndAction>().get()
        );
    }


};





int main(int argc,const char ** argv){
   Parser parser;
   parser.init(argc,argv);

}